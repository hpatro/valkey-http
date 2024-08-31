use rouille::input::HttpAuthCredentials;
use rouille::websocket;
use rouille::Request;
use serde::{Serialize, Deserialize};
use std::ptr::NonNull;
use std::thread;
use std::thread::sleep;
use std::time::Duration;
use rouille::try_or_400;
use rouille::router;
use rouille::Response;
use valkey_module::{
    logging::{log_debug, log_notice},
    raw, valkey_module, Context, KeyMode, Status, ThreadSafeContext, ValkeyString
};

pub const MODULE_NAME: &str = "valkeyhttp";

fn initialize(_ctx: &Context, _args: &[ValkeyString]) -> Status {
    thread::spawn(move || {
        start_http_handler();
    });
    Status::Ok
}

fn deinitialize(_ctx: &Context) -> Status {
    // Clean up resources if needed
    Status::Ok
}

#[derive(Deserialize)]
#[derive(Serialize)]
struct CommandRequest {
    args: String,
}

#[derive(Deserialize)]
#[derive(Serialize)]
struct CommandResponse<'a> {
    code: &'a str,
    data: Option<String>
}

impl<'a> Into<String> for CommandResponse<'a> {
    fn into(self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}

fn start_http_handler() {
    log_notice("Listening for HTTP request on 8080");
    rouille::start_server("0.0.0.0:8080", move |request| {
        let cred = match verify_auth(request) {
            Ok(cred) => cred,
            Err(_) => return Response::basic_http_auth_login_required("acl"),
        };
        router!(request,
            (POST) (/process) => {
                let command: CommandRequest = try_or_400!(rouille::input::json_input(request));
                log_debug(format!("Command to process: {}", command.args));
                Response::json(&process_command(cred.login.clone() ,command.args))
            },
            (GET) (/item/{key: String}) => {
                Response::json(&process_command(cred.login.clone(), format!("GET {}", key)))
            },
            (DELETE) (/item/{key: String}) => {
                Response::json(&process_command(cred.login.clone(), format!("DEL {}", key)))
            },
            (GET) (/ping) => {
                Response::json(&CommandResponse {code: "PONG", data: None})
            },
            (GET) (/health) => {
                // This is the websockets route.

                // In order to start using websockets we call `websocket::start`.
                // The function returns an error if the client didn't request websockets, in which
                // case we return an error 400 to the client thanks to the `try_or_400!` macro.
                //
                // The function returns a response to send back as part of the `start_server`
                // function, and a `websocket` variable of type `Receiver<Websocket>`.
                // Once the response has been sent back to the client, the `Receiver` will be
                // filled by rouille with a `Websocket` object representing the websocket.
                println!("{:?}", request.headers());
                let (response, websocket) = try_or_400!(websocket::start(request, Some("echo")));

                // Because of the nature of I/O in Rust, we need to spawn a separate thread for
                // each websocket.
                thread::spawn(move || {
                    // This line will block until the `response` above has been returned.
                    let ws = websocket.recv().unwrap();
                    // We use a separate function for better readability.
                    websocket_handling_thread(ws);
                });
                response
            },
            _ => Response::empty_404()
        )
    });
}

fn websocket_handling_thread(mut websocket: websocket::Websocket) {
    loop {
        // We wait for a new message to come from the websocket.
        let msg = match websocket.next() {
            Some(msg) => {
                match msg {
                    websocket::Message::Text(arguments) => {
                        if arguments.eq("PING") {
                            websocket.send_text("PONG");
                        } else {
                            let response = process_command("default".to_string(), arguments);
                            let response: String = response.into();
                            websocket.send_text(&response);
                        }
                    },
                    websocket::Message::Binary(_) => return,
                }
            }
            None => return,
        };
    }
}

fn websocket_health_thread(mut websocket: websocket::Websocket) {
    loop {
        websocket.send_text("PING").unwrap();
        sleep(Duration::from_secs(1));
    }
}


fn verify_auth(request: &Request) -> Result<HttpAuthCredentials, &'static str> {
    let auth = match rouille::input::basic_http_auth(request) {
        Some(a) => a,
        None => return Err("Credentials not found"),
    };
    let thread_ctx = ThreadSafeContext::new();
    let ctx = thread_ctx.lock();
    match ctx.call("AUTH", &[&auth.login, &auth.password]) {
        Ok(_) => return Ok(auth),
        Err(_) => return Err("Incorrect credentials"),
    };
}

fn process_command(_user_name: String, arguments: String) -> CommandResponse<'static> {
    let thread_ctx = ThreadSafeContext::new();
    let ctx = thread_ctx.lock();
/*
    // TODO Add user to the context for ACL validation on the call.
    /* Unable to add user to the context as the thread context doesn't have client attached to it. */
    let user_name = ValkeyString::create(None, user_name);
    let ctx_user_scope = match ctx.authenticate_user(&user_name) {
        Ok(ctx) => ctx,
        Err(msg) => return Response::basic_http_auth_login_required(msg.to_string().as_str()),
    };
 */
    let mut args = arguments.as_str().split(" ");
    let cmd_name = args.next().unwrap();
    let key = args.next().unwrap();
    let key = ValkeyString::create(NonNull::new(ctx.get_raw()), key);
    match cmd_name.to_lowercase().as_str() {
        "set" => {
            let key_inner = raw::open_key(ctx.get_raw(), key.inner, KeyMode::WRITE);
            let val = args.next().unwrap();
            let val = ValkeyString::create(NonNull::new(ctx.get_raw()), val);
            match raw::string_set(key_inner, val.inner) {
                raw::Status::Ok => CommandResponse {code: "OK", data: None},
                raw::Status::Err => CommandResponse {code: "ERR", data: None},
            }
        },
        "get" => {
            let key = ctx.open_key(&key);
            match key.read() {
                Ok(val) => {
                    if val.is_none() {
                        CommandResponse {code: "OK", data: None}
                    } else {
                        let val_data = String::from_utf8_lossy(val.unwrap()).into_owned();
                        CommandResponse {code: "OK", data: Some(val_data)}
                    }
                }
                Err(_) => {
                    CommandResponse {code: "ERR", data: None}
                }
            }
        },
        "del" => {
            let key = ctx.open_key_writable(&key);
            if key.is_empty() {
                return CommandResponse {code: "OK", data: Some("0".to_string())};
            }
            let _ = key.delete();
            CommandResponse {code: "OK", data: Some("1".to_string())}
        },
        _ => CommandResponse {code: "ERR", data: None },
    }

}

valkey_module! {
    name: MODULE_NAME,
    version: 1,
    allocator: (valkey_module::alloc::ValkeyAlloc, valkey_module::alloc::ValkeyAlloc),
    data_types: [],
    init: initialize,
    deinit: deinitialize,
    commands: [],
    configurations: [
        i64: [],
        string: [],
        bool: [],
        enum: [],
        module_args_as_configuration: true,
    ]
}