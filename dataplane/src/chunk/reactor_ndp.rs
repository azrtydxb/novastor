//! Reactor-native NDP client using SPDK sockets.
//!
//! Runs entirely on the SPDK reactor thread — zero thread crossings.
//! Uses spdk_sock for non-blocking TCP I/O, polled via spdk_sock_group
//! registered as a reactor poller.
//!
//! Each NDP peer has one persistent spdk_sock connection.
//! Requests are queued and responses processed in the reactor poll loop.

use std::collections::HashMap;
use std::ffi::CString;
use std::os::raw::{c_char, c_int, c_void};
use std::sync::OnceLock;

use log::{error, info, warn};
use ndp::header::NDP_HEADER_SIZE;
use ndp::{NdpHeader, NdpOp};

// SPDK socket + poller FFI bindings (subset of spdk/sock.h + spdk/thread.h).
mod ffi {
    use std::os::raw::{c_char, c_int, c_void};

    // Opaque SPDK types — we only use pointers.
    pub type spdk_sock = c_void;
    pub type spdk_sock_group = c_void;
    pub type spdk_poller = c_void;
    // bdev_io is opaque — we only pass pointers.
    pub type spdk_bdev_io = c_void;

    extern "C" {
        // Socket API
        pub fn spdk_sock_connect(
            ip: *const c_char,
            port: c_int,
            impl_name: *const c_char,
        ) -> *mut spdk_sock;

        pub fn spdk_sock_close(sock: *mut *mut spdk_sock) -> c_int;

        pub fn spdk_sock_writev(
            sock: *mut spdk_sock,
            iov: *mut libc::iovec,
            iovcnt: c_int,
        ) -> isize;

        pub fn spdk_sock_readv(sock: *mut spdk_sock, iov: *mut libc::iovec, iovcnt: c_int)
            -> isize;

        // Socket group API
        pub fn spdk_sock_group_create(ctx: *mut c_void) -> *mut spdk_sock_group;

        pub fn spdk_sock_group_add_sock(
            group: *mut spdk_sock_group,
            sock: *mut spdk_sock,
            cb_fn: Option<unsafe extern "C" fn(*mut c_void, *mut spdk_sock_group, *mut spdk_sock)>,
            cb_arg: *mut c_void,
        ) -> c_int;

        pub fn spdk_sock_group_remove_sock(
            group: *mut spdk_sock_group,
            sock: *mut spdk_sock,
        ) -> c_int;

        pub fn spdk_sock_group_poll(group: *mut spdk_sock_group) -> c_int;

        // Poller API
        pub fn spdk_poller_register(
            fn_ptr: Option<unsafe extern "C" fn(*mut c_void) -> c_int>,
            arg: *mut c_void,
            period_usecs: u64,
        ) -> *mut spdk_poller;

        // bdev_io completion
        pub fn spdk_bdev_io_complete(bdev_io: *mut spdk_bdev_io, status: i32);
    }
}

// NDP_HEADER_SIZE imported from ndp::header

/// Maximum in-flight requests per peer.
const MAX_INFLIGHT: usize = 64;

/// A pending NDP request waiting for a response.
struct PendingRequest {
    /// The bdev_io to complete when response arrives (opaque *mut spdk_bdev_io).
    bdev_io: *mut c_void,
    /// Expected operation type in response.
    expected_op: NdpOp,
    /// IOV descriptors for scatter into (reads only).
    iovs: Option<Vec<(usize, usize)>>,
    /// Offset within first sub-block (reads only).
    buf_offset: usize,
    /// Total bytes requested (reads only).
    total_len: usize,
}

// SAFETY: Only accessed from single SPDK reactor thread.
unsafe impl Send for PendingRequest {}
unsafe impl Sync for PendingRequest {}

/// Per-peer NDP connection state on the reactor.
struct ReactorNdpPeer {
    /// SPDK socket handle.
    sock: *mut c_void, // spdk_sock*
    /// Peer address for reconnection.
    addr: String,
    port: u16,
    /// Pending requests awaiting responses (FIFO — NDP is ordered per connection).
    pending: Vec<PendingRequest>,
    /// Partial receive buffer for assembling NDP response messages.
    recv_buf: Vec<u8>,
    /// Partial send buffer for writes that couldn't complete in one call.
    send_buf: Vec<u8>,
}

/// Global reactor-native NDP state. Only accessed from reactor thread.
static REACTOR_NDP: OnceLock<std::sync::Mutex<Option<ReactorNdpState>>> = OnceLock::new();

struct ReactorNdpState {
    /// Socket group for polling all peer connections.
    sock_group: *mut c_void, // spdk_sock_group*
    /// Poller handle.
    poller: *mut c_void, // spdk_poller*
    /// Connected peers by address ("ip:port").
    peers: HashMap<String, ReactorNdpPeer>,
}

// SAFETY: Only accessed from single SPDK reactor thread.
unsafe impl Send for ReactorNdpState {}
unsafe impl Sync for ReactorNdpState {}

/// Initialize the reactor NDP subsystem. Must be called from the reactor thread.
pub unsafe fn init() {
    let cell = REACTOR_NDP.get_or_init(|| std::sync::Mutex::new(None));
    let mut guard = cell.lock().unwrap();
    if guard.is_some() {
        return; // Already initialized.
    }

    // Create socket group for polling.
    let sock_group = ffi::spdk_sock_group_create(std::ptr::null_mut());
    if sock_group.is_null() {
        error!("reactor_ndp: failed to create sock_group");
        return;
    }

    // Register poller — called on every reactor iteration.
    let poller = ffi::spdk_poller_register(
        Some(ndp_poller_fn),
        sock_group as *mut c_void,
        0, // poll every iteration
    );

    info!(
        "reactor_ndp: initialized (sock_group={:p}, poller={:p})",
        sock_group, poller
    );

    *guard = Some(ReactorNdpState {
        sock_group,
        poller,
        peers: HashMap::new(),
    });
}

/// Connect to an NDP peer. Must be called from the reactor thread.
pub unsafe fn connect_peer(addr: &str, port: u16) -> bool {
    let cell = REACTOR_NDP.get_or_init(|| std::sync::Mutex::new(None));
    let mut guard = cell.lock().unwrap();
    let state = match guard.as_mut() {
        Some(s) => s,
        None => return false,
    };

    let key = format!("{}:{}", addr, port);
    if state.peers.contains_key(&key) {
        return true; // Already connected.
    }

    let addr_c = match CString::new(addr) {
        Ok(c) => c,
        Err(_) => return false,
    };

    let sock = ffi::spdk_sock_connect(
        addr_c.as_ptr(),
        port as c_int,
        std::ptr::null(), // default impl (posix)
    );
    if sock.is_null() {
        warn!("reactor_ndp: connect to {}:{} failed", addr, port);
        return false;
    }

    // Add to sock group for polling.
    let rc = ffi::spdk_sock_group_add_sock(
        state.sock_group,
        sock,
        Some(ndp_sock_cb),
        std::ptr::null_mut(), // cb_arg — we look up by sock pointer
    );
    if rc != 0 {
        warn!(
            "reactor_ndp: add_sock failed for {}:{}: rc={}",
            addr, port, rc
        );
        ffi::spdk_sock_close(&mut (sock as *mut c_void) as *mut *mut c_void as *mut *mut _);
        return false;
    }

    info!("reactor_ndp: connected to {}:{}", addr, port);

    state.peers.insert(
        key,
        ReactorNdpPeer {
            sock,
            addr: addr.to_string(),
            port,
            pending: Vec::with_capacity(MAX_INFLIGHT),
            recv_buf: Vec::with_capacity(NDP_HEADER_SIZE + 65536),
            send_buf: Vec::new(),
        },
    );

    true
}

/// Send an NDP read request on the reactor. Returns true if queued.
/// `bdev_io` is an opaque pointer (*mut spdk_bdev_io) passed through for completion.
pub unsafe fn send_read(
    peer_addr: &str,
    volume_hash: u64,
    offset: u64,
    length: u32,
    bdev_io: *mut c_void,
    iovs: Vec<(usize, usize)>,
    buf_offset: usize,
) -> bool {
    let cell = match REACTOR_NDP.get() {
        Some(c) => c,
        None => return false,
    };
    let mut guard = cell.lock().unwrap();
    let state = match guard.as_mut() {
        Some(s) => s,
        None => return false,
    };

    let peer = match state.peers.get_mut(peer_addr) {
        Some(p) => p,
        None => return false,
    };

    if peer.pending.len() >= MAX_INFLIGHT {
        return false; // Too many in-flight.
    }

    // Build NDP read request header.
    let header = NdpHeader::request(NdpOp::Read, 0, volume_hash, offset, length);
    let mut header_bytes = [0u8; NDP_HEADER_SIZE];
    header.encode(&mut header_bytes);

    // Send header (no data payload for reads).
    let mut iov = libc::iovec {
        iov_base: header_bytes.as_ptr() as *mut c_void,
        iov_len: NDP_HEADER_SIZE,
    };

    let written = ffi::spdk_sock_writev(peer.sock, &mut iov, 1);
    if written < 0 || written as usize != NDP_HEADER_SIZE {
        warn!(
            "reactor_ndp: partial write to {}, written={}",
            peer_addr, written
        );
        // TODO: buffer partial writes. For now, fail.
        return false;
    }

    // Queue pending request.
    peer.pending.push(PendingRequest {
        bdev_io,
        expected_op: NdpOp::ReadResp,
        iovs: Some(iovs),
        buf_offset,
        total_len: length as usize,
    });

    true
}

/// Send an NDP write request on the reactor. Returns true if queued.
pub unsafe fn send_write(
    peer_addr: &str,
    volume_hash: u64,
    offset: u64,
    data: &[u8],
    bdev_io: *mut c_void,
) -> bool {
    let cell = match REACTOR_NDP.get() {
        Some(c) => c,
        None => return false,
    };
    let mut guard = cell.lock().unwrap();
    let state = match guard.as_mut() {
        Some(s) => s,
        None => return false,
    };

    let peer = match state.peers.get_mut(peer_addr) {
        Some(p) => p,
        None => return false,
    };

    if peer.pending.len() >= MAX_INFLIGHT {
        return false;
    }

    // Build NDP write request header + data.
    let header = NdpHeader::request(NdpOp::Write, 0, volume_hash, offset, data.len() as u32);
    let mut header_bytes = [0u8; NDP_HEADER_SIZE];
    header.encode(&mut header_bytes);

    // Send header + data as two iovecs.
    let mut iovs = [
        libc::iovec {
            iov_base: header_bytes.as_ptr() as *mut c_void,
            iov_len: NDP_HEADER_SIZE,
        },
        libc::iovec {
            iov_base: data.as_ptr() as *mut c_void,
            iov_len: data.len(),
        },
    ];

    let total = NDP_HEADER_SIZE + data.len();
    let written = ffi::spdk_sock_writev(peer.sock, iovs.as_mut_ptr(), 2);
    if written < 0 || written as usize != total {
        warn!(
            "reactor_ndp: partial write to {}, written={}/{}",
            peer_addr, written, total
        );
        return false;
    }

    peer.pending.push(PendingRequest {
        bdev_io,
        expected_op: NdpOp::WriteResp,
        iovs: None,
        buf_offset: 0,
        total_len: 0,
    });

    true
}

/// Check if a peer is connected.
pub fn is_connected(peer_addr: &str) -> bool {
    let cell = match REACTOR_NDP.get() {
        Some(c) => c,
        None => return false,
    };
    let guard = cell.lock().unwrap();
    match guard.as_ref() {
        Some(s) => s.peers.contains_key(peer_addr),
        None => false,
    }
}

/// Poller function — called by SPDK reactor on every iteration.
unsafe extern "C" fn ndp_poller_fn(arg: *mut c_void) -> c_int {
    let sock_group = arg; // spdk_sock_group*
    let rc = ffi::spdk_sock_group_poll(sock_group);
    if rc < 0 {
        // Error — but don't spam logs. Errors are handled per-socket.
        0
    } else {
        rc
    }
}

/// Socket callback — invoked when a socket has data ready.
unsafe extern "C" fn ndp_sock_cb(
    _arg: *mut c_void,
    _group: *mut c_void, // spdk_sock_group*
    sock: *mut c_void,   // spdk_sock*
) {
    // Read available data from the socket.
    let mut buf = [0u8; 65536 + NDP_HEADER_SIZE];
    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr() as *mut c_void,
        iov_len: buf.len(),
    };

    let n = ffi::spdk_sock_readv(sock, &mut iov, 1);
    if n <= 0 {
        if n == 0 {
            warn!("reactor_ndp: peer disconnected");
        }
        // TODO: handle disconnect / reconnect
        return;
    }

    let received = &buf[..n as usize];

    // Find which peer this socket belongs to and process the response.
    let cell = match REACTOR_NDP.get() {
        Some(c) => c,
        None => return,
    };
    let mut guard = cell.lock().unwrap();
    let state = match guard.as_mut() {
        Some(s) => s,
        None => return,
    };

    // Find peer by socket pointer.
    let peer = match state
        .peers
        .values_mut()
        .find(|p| std::ptr::eq(p.sock, sock))
    {
        Some(p) => p,
        None => return,
    };

    // Append to recv buffer and try to parse complete NDP messages.
    peer.recv_buf.extend_from_slice(received);

    loop {
        if peer.recv_buf.len() < NDP_HEADER_SIZE {
            break; // Need more data for header.
        }

        // Parse header to get data_length.
        let header_arr: &[u8; NDP_HEADER_SIZE] = match peer.recv_buf[..NDP_HEADER_SIZE].try_into() {
            Ok(a) => a,
            Err(_) => break,
        };
        let header = match NdpHeader::decode(header_arr) {
            Ok(h) => h,
            Err(e) => {
                error!("reactor_ndp: invalid NDP header: {}, dropping", e);
                peer.recv_buf.clear();
                break;
            }
        };

        let total_msg_len = NDP_HEADER_SIZE + header.data_length as usize;
        if peer.recv_buf.len() < total_msg_len {
            break; // Need more data for payload.
        }

        // Extract the complete message.
        let data = if header.data_length > 0 {
            Some(peer.recv_buf[NDP_HEADER_SIZE..total_msg_len].to_vec())
        } else {
            None
        };

        // Remove processed bytes.
        peer.recv_buf.drain(..total_msg_len);

        // Match to pending request (FIFO order).
        if let Some(req) = peer.pending.first() {
            let bdev_io = req.bdev_io;

            if header.status != 0 {
                // Error response — complete bdev_io with failure.
                peer.pending.remove(0);
                ffi::spdk_bdev_io_complete(bdev_io, -1i32 /* SPDK_BDEV_IO_STATUS_FAILED */);
                continue;
            }

            match header.op {
                NdpOp::ReadResp => {
                    // Copy data to iovs.
                    if let Some(ref data) = data {
                        if let Some(ref iovs) = req.iovs {
                            let mut src_off = req.buf_offset;
                            for &(base, len) in iovs {
                                let to_copy =
                                    std::cmp::min(len, data.len().saturating_sub(src_off));
                                if to_copy > 0 {
                                    std::ptr::copy_nonoverlapping(
                                        data[src_off..].as_ptr(),
                                        base as *mut u8,
                                        to_copy,
                                    );
                                }
                                src_off += to_copy;
                            }
                        }
                    }
                    peer.pending.remove(0);
                    ffi::spdk_bdev_io_complete(
                        bdev_io, 0i32, /* SPDK_BDEV_IO_STATUS_SUCCESS */
                    );
                }
                NdpOp::WriteResp => {
                    peer.pending.remove(0);
                    ffi::spdk_bdev_io_complete(
                        bdev_io, 0i32, /* SPDK_BDEV_IO_STATUS_SUCCESS */
                    );
                }
                _ => {
                    // Unexpected response type.
                    peer.pending.remove(0);
                    ffi::spdk_bdev_io_complete(
                        bdev_io, -1i32, /* SPDK_BDEV_IO_STATUS_FAILED */
                    );
                }
            }
        }
    }
}
