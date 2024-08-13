use serde::Deserialize;
use serde_json::{Error, Value};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    ptr::NonNull,
    thread,
};
use valkey_module::{
    logging::{log_debug, log_notice},
    raw, valkey_module, Context, KeyMode, Status, ThreadSafeContext, ValkeyString,
};

pub const MODULE_NAME: &str = "valkeyhttp";

pub const PRE_200_RESPONSE: &str = "HTTP/1.1 200 OK\r\n\r\n";
pub const OK_RESPONSE: &str = "HTTP/1.1 200 OK\r\n\r\n";
pub const ERR_RESPONSE: &str = "HTTP/1.1 200 ERR\r\n\r\n";
pub const NOT_FOUND_RESPONSE: &str = "HTTP/1.1 404 ERR Not found\r\n\r\n";

fn initialize(_ctx: &Context, _args: &[ValkeyString]) -> Status {
    thread::spawn(move || {
        let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
        log_notice("listening via http interface on port 8080");
        for stream in listener.incoming() {
            let stream = stream.unwrap();
            log_debug("Connection established");
            thread::spawn(move || {
                handle_connection_lib(stream);
            });
        }
        Status::Ok
    });
    Status::Ok
}

fn handle_connection_lib(mut stream: TcpStream) {
    /*     let buf_reader = BufReader::new(&mut stream);
       let http_request = buf_reader
           .lines()
           .map(|result| result.unwrap())
           .take_while(|line| !line.is_empty())
           .collect();
    */

    let mut buf = [0; 1024];
    let bytes_read = stream.read(&mut buf).unwrap();

    let mut headers = [httparse::EMPTY_HEADER; 16];
    let mut req = httparse::Request::new(&mut headers);
    let header_end = req.parse(&buf).unwrap();
    let mut content_length = 0;
    for header in headers {
        if header.name == "Content-Length" {
            content_length = std::str::from_utf8(header.value)
                .unwrap()
                .parse::<i32>()
                .unwrap();
        }
    }
    let mut content_length: usize = content_length as usize;
    let body = &buf[header_end.unwrap()..];
    let body_str: &str = std::str::from_utf8(body)
        .unwrap()
        .split_at(content_length)
        .0;
    println!("Body: {}, length: {}", body_str, body_str.len());
    let v: Result<Value, Error> = serde_json::from_str(body_str);
    match v {
        Ok(cmd) => {
            let thread_ctx = ThreadSafeContext::new();
            let ctx = thread_ctx.lock();
            let mut args = cmd["command"].as_str().unwrap().split(" ");
            let cmd_name = args.next().unwrap();
            let key = args.next().unwrap();
            let key = ValkeyString::create(NonNull::new(ctx.get_raw()), key);
            match cmd_name.to_lowercase().as_str() {
                "set" => {
                    let key_inner = raw::open_key(ctx.get_raw(), key.inner, KeyMode::WRITE);
                    let val = args.next().unwrap();
                    let val = ValkeyString::create(NonNull::new(ctx.get_raw()), val);
                    match raw::string_set(key_inner, val.inner) {
                        raw::Status::Ok => stream.write(OK_RESPONSE.as_bytes()),
                        raw::Status::Err => stream.write(ERR_RESPONSE.as_bytes()),
                    };
                }
                "get" => {
                    let key = ctx.open_key(&key);
                    match key.read() {
                        Ok(val) => {
                            if val.is_none() {
                                stream.write(NOT_FOUND_RESPONSE.as_bytes());
                            } else {
                                let val_data = String::from_utf8_lossy(val.unwrap()).into_owned();
                                let val_len = val_data.len();
                                let response = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
                                    val_len, val_data
                                );
                                println!("{}", response);
                                stream.write(response.as_bytes());
                            }
                        }
                        Err(_) => {
                            stream.write(ERR_RESPONSE.as_bytes());
                        }
                    }
                }
                _ => todo!(),
            }
        }
        Err(e) => println!("Malformed data {}", e),
    }
}

fn deinitialize(_ctx: &Context) -> Status {
    // Clean up resources if needed
    Status::Ok
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