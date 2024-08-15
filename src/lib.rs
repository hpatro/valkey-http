use serde::{Serialize, Deserialize};
use std::ptr::NonNull;
use std::thread;
use rouille::try_or_400;
use rouille::router;
use rouille::Response;
use valkey_module::{
    logging::{log_debug, log_notice},
    raw, valkey_module, Context, KeyMode, Status, ThreadSafeContext, ValkeyString,
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

fn start_http_handler() {
    log_notice("Listening for HTTP request on 8080");
    rouille::start_server("0.0.0.0:8080", move |request| {
        router!(request,
            (POST) (/process) => {
                let command: CommandRequest = try_or_400!(rouille::input::json_input(request));
                log_debug(format!("Command to process: {}", command.args));
                process_command(command.args)
            },
            _ => Response::empty_404()
        )
    });
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

fn process_command(arguments: String) -> Response {
    let thread_ctx = ThreadSafeContext::new();
    let ctx = thread_ctx.lock();
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
                raw::Status::Ok => Response::json(&CommandResponse {code: "OK", data: None}),
                raw::Status::Err => Response::json(&CommandResponse {code: "ERR", data: None}),
            }
        }
        "get" => {
            let key = ctx.open_key(&key);
            match key.read() {
                Ok(val) => {
                    if val.is_none() {
                        Response::json(&CommandResponse {code: "OK", data: None})
                    } else {
                        let val_data = String::from_utf8_lossy(val.unwrap()).into_owned();
                        Response::json(&CommandResponse {code: "OK", data: Some(val_data)})
                    }
                }
                Err(_) => {
                    Response::json(&CommandResponse {code: "ERR", data: None})
                }
            }
        },
        _ => Response::empty_404(),
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