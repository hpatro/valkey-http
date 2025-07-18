use rouille::input::HttpAuthCredentials;
use rouille::websocket;
use rouille::Request;
use serde::{Deserialize, Serialize};
use std::ptr::NonNull;
use std::sync::mpsc::{self, Sender};
use std::thread;
use std::thread::sleep;
use std::time::Duration;
use rouille::try_or_400;
use rouille::router;
use rouille::Response;
use valkey_module::{
    CommandFilter, CommandFilterCtx,
    logging::{log_debug, log_notice},
    raw, valkey_module, Context, KeyMode, Status, ThreadSafeContext, ValkeyString,
};
use once_cell::sync::Lazy;
use std::sync::Mutex;

pub const MODULE_NAME: &str = "valkeyhttp";

static MONITOR_CHANNELS: Lazy<Mutex<Vec<Sender<String>>>> = Lazy::new(|| Mutex::new(Vec::new()));

static mut CMD_FILTER: Option<CommandFilter> = None;

fn initialize(_ctx: &Context, _args: &[ValkeyString]) -> Status {
    unsafe {
        let filter = _ctx.register_command_filter(command_filter_callback, 0);
        CMD_FILTER = Some(filter);
    }
    thread::spawn(move || {
        start_http_handler();
    });
    Status::Ok
}

fn deinitialize(_ctx: &Context) -> Status {
    unsafe {
        if let Some(filter) = CMD_FILTER {
            _ctx.unregister_command_filter(&filter);
            CMD_FILTER = None;
        }
    }
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
enum ValkeyCommandCode {
    Ok,
    Err,
    NotFound,
    NotAllowed,
}

#[derive(Deserialize)]
#[derive(Serialize)]
struct CommandResponse {
    code: ValkeyCommandCode,
    data: Option<String>
}

impl Into<String> for CommandResponse {
    fn into(self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}

extern "C" fn command_filter_callback(filter: *mut valkey_module::RedisModuleCommandFilterCtx) {
    let ctx = CommandFilterCtx::new(filter);
    let cmd = match ctx.cmd_get_try_as_str() {
        Ok(c) => c,
        Err(_) => return,
    };
    let mut parts = vec![cmd];
    parts.extend(ctx.get_all_args_wo_cmd());
    let message = parts.join(" ");
    let mut senders = MONITOR_CHANNELS.lock().unwrap();
    senders.retain(|s| s.send(message.clone()).is_ok());
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
                Response::json(&CommandResponse {code: ValkeyCommandCode::Ok, data: Some("PONG".to_string())})
            },
            (GET) (/health) => {
                let (response, websocket) = try_or_400!(websocket::start(request, Some("echo")));
                thread::spawn(move || {
                    let ws = websocket.recv().unwrap();
                    websocket_health_thread(ws);
                });
                response
            },
            (GET) (/process) => {
                let (response, websocket) = try_or_400!(websocket::start(request, Some("echo")));
                thread::spawn(move || {
                    let ws = websocket.recv().unwrap();
                    websocket_handling_thread(ws);
                });
                response
            },
            (GET) (/monitor) => {
                let (response, websocket) = try_or_400!(websocket::start(request, Some("echo")));
                let (tx, rx) = mpsc::channel();
                MONITOR_CHANNELS.lock().unwrap().push(tx);
                thread::spawn(move || {
                    let ws = websocket.recv().unwrap();
                    websocket_monitor_thread(ws, rx);
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
        match websocket.next() {
            Some(msg) => {
                match msg {
                    websocket::Message::Text(arguments) => {
                        if arguments.to_lowercase().eq("ping") {
                            let _ = websocket.send_text("PONG");
                        } else {
                            let response = process_command("default".to_string(), arguments);
                            let response: String = response.into();
                            let _ = websocket.send_text(&response);
                        }
                    },
                    websocket::Message::Binary(_) => return,
                }
            }
            None => {
                log_debug("Websocket connection closed!");
                return;
            }
        };
    }
}

fn websocket_health_thread(mut websocket: websocket::Websocket) {
    loop {
        websocket.send_text("PING").unwrap();
        sleep(Duration::from_secs(1));
    }
}

fn websocket_monitor_thread(mut websocket: websocket::Websocket, rx: mpsc::Receiver<String>) {
    for msg in rx {
        if websocket.send_text(&msg).is_err() {
            break;
        }
    }
    log_debug("Monitor websocket connection closed!");
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

fn process_command(_user_name: String, arguments: String) -> CommandResponse {
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
                raw::Status::Ok => CommandResponse {code: ValkeyCommandCode::Ok, data: None},
                raw::Status::Err => CommandResponse {code: ValkeyCommandCode::Err, data: None},
            }
        },
        "get" => {
            let key = ctx.open_key(&key);
            match key.read() {
                Ok(val) => {
                    if val.is_none() {
                        CommandResponse {code: ValkeyCommandCode::Ok, data: None}
                    } else {
                        let val_data = String::from_utf8_lossy(val.unwrap()).into_owned();
                        CommandResponse {code: ValkeyCommandCode::Ok, data: Some(val_data)}
                    }
                }
                Err(_) => {
                    CommandResponse {code: ValkeyCommandCode::Err, data: None}
                }
            }
        },
        "del" => {
            let key = ctx.open_key_writable(&key);
            if key.is_empty() {
                return CommandResponse {code: ValkeyCommandCode::Ok, data: Some("0".to_string())};
            }
            let _ = key.delete();
            CommandResponse {code: ValkeyCommandCode::Ok, data: Some("1".to_string())}
        },
        _ => CommandResponse {code: ValkeyCommandCode::Err, data: None },
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