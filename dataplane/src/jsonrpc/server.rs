use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::path::Path;
use std::sync::Arc;
use std::thread;

use serde::{Deserialize, Serialize};

use crate::error::DataPlaneError;

/// JSON-RPC 2.0 request envelope.
#[derive(Debug, Deserialize)]
pub struct Request {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: serde_json::Value,
    pub id: serde_json::Value,
}

/// JSON-RPC 2.0 response envelope.
#[derive(Debug, Serialize)]
pub struct Response {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
    pub id: serde_json::Value,
}

/// JSON-RPC 2.0 error object.
#[derive(Debug, Serialize)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
}

impl Response {
    pub fn success(id: serde_json::Value, result: serde_json::Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: Some(result),
            error: None,
            id,
        }
    }

    pub fn error(id: serde_json::Value, code: i64, message: String) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(RpcError { code, message }),
            id,
        }
    }
}

/// Method handler function signature.
pub type MethodHandler = Arc<dyn Fn(serde_json::Value) -> Result<serde_json::Value, DataPlaneError> + Send + Sync>;

/// Router dispatches JSON-RPC methods to their handlers.
pub struct Router {
    handlers: std::collections::HashMap<String, MethodHandler>,
}

impl Router {
    pub fn new() -> Self {
        Self {
            handlers: std::collections::HashMap::new(),
        }
    }

    pub fn register(&mut self, method: &str, handler: MethodHandler) {
        self.handlers.insert(method.to_string(), handler);
    }

    pub fn dispatch(&self, req: &Request) -> Response {
        match self.handlers.get(&req.method) {
            Some(handler) => match handler(req.params.clone()) {
                Ok(result) => Response::success(req.id.clone(), result),
                Err(e) => Response::error(req.id.clone(), -32000, e.to_string()),
            },
            None => Response::error(
                req.id.clone(),
                -32601,
                format!("method not found: {}", req.method),
            ),
        }
    }
}

/// Start the JSON-RPC server on the given Unix domain socket path.
///
/// This function spawns a background thread that accepts connections and
/// dispatches requests via the router. Returns a handle to the listener
/// thread.
pub fn start_server(
    socket_path: &str,
    router: Arc<Router>,
) -> Result<thread::JoinHandle<()>, DataPlaneError> {
    // Remove stale socket if it exists.
    let path = Path::new(socket_path);
    if path.exists() {
        std::fs::remove_file(path).map_err(|e| {
            DataPlaneError::JsonRpcError(format!("removing stale socket: {e}"))
        })?;
    }

    let listener = UnixListener::bind(path).map_err(|e| {
        DataPlaneError::JsonRpcError(format!("binding socket {socket_path}: {e}"))
    })?;

    let handle = thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let router = Arc::clone(&router);
                    thread::spawn(move || handle_connection(stream, &router));
                }
                Err(e) => {
                    eprintln!("json-rpc: accept error: {e}");
                }
            }
        }
    });

    Ok(handle)
}

fn handle_connection(stream: std::os::unix::net::UnixStream, router: &Router) {
    let reader = BufReader::new(&stream);
    let mut writer = &stream;

    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        if line.trim().is_empty() {
            continue;
        }

        let response = match serde_json::from_str::<Request>(&line) {
            Ok(req) => router.dispatch(&req),
            Err(e) => Response::error(
                serde_json::Value::Null,
                -32700,
                format!("parse error: {e}"),
            ),
        };

        let mut out = serde_json::to_vec(&response).unwrap_or_default();
        out.push(b'\n');
        if writer.write_all(&out).is_err() {
            break;
        }
    }
}
