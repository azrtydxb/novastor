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
    /// Unique request ID — matches response's request_id field.
    request_id: u64,
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

/// Global request ID counter — monotonically increasing, unique per reactor.
static NEXT_REQUEST_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

fn next_request_id() -> u64 {
    NEXT_REQUEST_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
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
        50, // poll every 50μs — fast enough for NDP responses, low enough overhead
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

    // Lazy-connect: if not connected, connect now and add to sock_group.
    if !state.peers.contains_key(peer_addr) {
        if let Some((ip, port_str)) = peer_addr.rsplit_once(':') {
            if let Ok(port) = port_str.parse::<u16>() {
                let addr_c = match CString::new(ip) {
                    Ok(c) => c,
                    Err(_) => return false,
                };
                let sock = ffi::spdk_sock_connect(addr_c.as_ptr(), port as c_int, std::ptr::null());
                if sock.is_null() {
                    return false;
                }
                let rc = ffi::spdk_sock_group_add_sock(
                    state.sock_group,
                    sock,
                    Some(ndp_sock_cb),
                    std::ptr::null_mut(),
                );
                if rc != 0 {
                    let mut s = sock;
                    ffi::spdk_sock_close(&mut s);
                    return false;
                }
                info!("reactor_ndp: lazy-connected to {}", peer_addr);
                state.peers.insert(
                    peer_addr.to_string(),
                    ReactorNdpPeer {
                        sock,
                        addr: ip.to_string(),
                        port,
                        pending: Vec::with_capacity(MAX_INFLIGHT),
                        recv_buf: Vec::with_capacity(NDP_HEADER_SIZE + 65536),
                        send_buf: Vec::new(),
                    },
                );
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    let peer = state.peers.get_mut(peer_addr).unwrap();

    if peer.pending.len() >= MAX_INFLIGHT {
        return false;
    }

    // Build NDP read request header with unique request_id.
    let rid = next_request_id();
    let header = NdpHeader::request(NdpOp::Read, rid, volume_hash, offset, length);
    let mut header_bytes = [0u8; NDP_HEADER_SIZE];
    header.encode(&mut header_bytes);

    // Send header (no data payload for reads).
    let mut iov = libc::iovec {
        iov_base: header_bytes.as_ptr() as *mut c_void,
        iov_len: NDP_HEADER_SIZE,
    };

    let written = ffi::spdk_sock_writev(peer.sock, &mut iov, 1);
    if written < 0 || written as usize != NDP_HEADER_SIZE {
        // Connection may have failed — remove and let next attempt reconnect.
        if let Some(mut dead) = state.peers.remove(peer_addr) {
            ffi::spdk_sock_group_remove_sock(state.sock_group, dead.sock);
            ffi::spdk_sock_close(&mut dead.sock);
        }
        return false;
    }

    // Queue pending request with request_id for response matching.
    peer.pending.push(PendingRequest {
        request_id: rid,
        bdev_io,
        expected_op: NdpOp::ReadResp,
        iovs: Some(iovs),
        buf_offset,
        total_len: length as usize,
    });

    true
}

/// Send an NDP write request on the reactor. Returns true if queued.
/// Handles partial writes by buffering unsent bytes in the peer's send_buf.
/// The poller drains send_buf on subsequent iterations.
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

    // Lazy-connect if needed.
    if !state.peers.contains_key(peer_addr) {
        if let Some((ip, port_str)) = peer_addr.rsplit_once(':') {
            if let Ok(port) = port_str.parse::<u16>() {
                let addr_c = match CString::new(ip) {
                    Ok(c) => c,
                    Err(_) => return false,
                };
                let sock = ffi::spdk_sock_connect(addr_c.as_ptr(), port as c_int, std::ptr::null());
                if sock.is_null() {
                    return false;
                }
                let rc = ffi::spdk_sock_group_add_sock(
                    state.sock_group,
                    sock,
                    Some(ndp_sock_cb),
                    std::ptr::null_mut(),
                );
                if rc != 0 {
                    let mut s = sock;
                    ffi::spdk_sock_close(&mut s);
                    return false;
                }
                info!("reactor_ndp: lazy-connected to {} (write)", peer_addr);
                state.peers.insert(
                    peer_addr.to_string(),
                    ReactorNdpPeer {
                        sock,
                        addr: ip.to_string(),
                        port,
                        pending: Vec::with_capacity(MAX_INFLIGHT),
                        recv_buf: Vec::with_capacity(NDP_HEADER_SIZE + 65536),
                        send_buf: Vec::new(),
                    },
                );
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    let peer = state.peers.get_mut(peer_addr).unwrap();

    if peer.pending.len() >= MAX_INFLIGHT {
        return false;
    }

    let rid = next_request_id();

    // If there's already unsent data in send_buf, queue this message
    // after it (FIFO ordering). The poller will drain it.
    if !peer.send_buf.is_empty() {
        let header = NdpHeader::request(NdpOp::Write, rid, volume_hash, offset, data.len() as u32);
        let mut hdr = [0u8; NDP_HEADER_SIZE];
        header.encode(&mut hdr);
        peer.send_buf.extend_from_slice(&hdr);
        peer.send_buf.extend_from_slice(data);
        peer.pending.push(PendingRequest {
            request_id: rid,
            bdev_io,
            expected_op: NdpOp::WriteResp,
            iovs: None,
            buf_offset: 0,
            total_len: 0,
        });
        return true;
    }

    // Build full message: header + data.
    let header = NdpHeader::request(NdpOp::Write, rid, volume_hash, offset, data.len() as u32);
    let mut header_bytes = [0u8; NDP_HEADER_SIZE];
    header.encode(&mut header_bytes);

    // Assemble into contiguous buffer for atomic-or-buffered send.
    let mut msg = Vec::with_capacity(NDP_HEADER_SIZE + data.len());
    msg.extend_from_slice(&header_bytes);
    msg.extend_from_slice(data);

    let mut iov = libc::iovec {
        iov_base: msg.as_ptr() as *mut c_void,
        iov_len: msg.len(),
    };

    let written = ffi::spdk_sock_writev(peer.sock, &mut iov, 1);
    if written < 0 {
        // Connection error — remove peer.
        if let Some(mut dead) = state.peers.remove(peer_addr) {
            ffi::spdk_sock_group_remove_sock(state.sock_group, dead.sock);
            ffi::spdk_sock_close(&mut dead.sock);
        }
        return false;
    }

    let written = written as usize;
    if written < msg.len() {
        // Partial write — buffer the remainder for the poller to drain.
        peer.send_buf.extend_from_slice(&msg[written..]);
    }

    peer.pending.push(PendingRequest {
        request_id: rid,
        bdev_io,
        expected_op: NdpOp::WriteResp,
        iovs: None,
        buf_offset: 0,
        total_len: 0,
    });

    true
}

/// Check if reactor NDP is available for this peer.
/// With lazy-connect, always returns true if reactor NDP is initialized —
/// send_read/send_write will connect on demand.
pub fn is_connected(peer_addr: &str) -> bool {
    let _ = peer_addr; // Lazy connect — don't check peer map.
    let cell = match REACTOR_NDP.get() {
        Some(c) => c,
        None => return false,
    };
    let guard = cell.lock().unwrap();
    guard.is_some() // Initialized = available. Lazy connect on first use.
}

/// Poller function — called by SPDK reactor at 50μs intervals.
/// Polls for incoming responses AND drains any pending send buffers.
/// Reads directly from each peer socket instead of relying on sock_group
/// callback — avoids callback delivery issues.
unsafe extern "C" fn ndp_poller_fn(_arg: *mut c_void) -> c_int {
    // Don't use spdk_sock_group_poll — read directly from each peer.
    let rc: c_int = 0;

    // Drain pending send buffers for all peers.
    let cell = match REACTOR_NDP.get() {
        Some(c) => c,
        None => return rc.max(0),
    };
    if let Ok(mut guard) = cell.try_lock() {
        if let Some(state) = guard.as_mut() {
            let mut dead_peers: Vec<String> = Vec::new();
            for (addr, peer) in state.peers.iter_mut() {
                if peer.send_buf.is_empty() {
                    continue;
                }
                let mut iov = libc::iovec {
                    iov_base: peer.send_buf.as_ptr() as *mut c_void,
                    iov_len: peer.send_buf.len(),
                };
                let written = ffi::spdk_sock_writev(peer.sock, &mut iov, 1);
                if written < 0 {
                    // Connection dead.
                    dead_peers.push(addr.clone());
                    continue;
                }
                let written = written as usize;
                if written > 0 {
                    peer.send_buf.drain(..written);
                }
            }
            // Clean up dead peers from send drain.
            for addr in &dead_peers {
                if let Some(mut dead) = state.peers.remove(addr) {
                    ffi::spdk_sock_group_remove_sock(state.sock_group, dead.sock);
                    ffi::spdk_sock_close(&mut dead.sock);
                    for req in dead.pending {
                        ffi::spdk_bdev_io_complete(req.bdev_io, -1i32 /* FAILED */);
                    }
                    warn!("reactor_ndp: peer {} died during send drain", addr);
                }
            }

            // Direct read from each peer socket — don't rely on sock_group callback.
            let mut read_dead: Vec<String> = Vec::new();
            let peer_addrs: Vec<String> = state.peers.keys().cloned().collect();
            for addr in &peer_addrs {
                if dead_peers.contains(addr) {
                    continue;
                }
                let peer = match state.peers.get_mut(addr) {
                    Some(p) => p,
                    None => continue,
                };
                // Only read if there are pending requests.
                if peer.pending.is_empty() {
                    continue;
                }
                let mut buf = [0u8; 65536 + NDP_HEADER_SIZE];
                let mut iov = libc::iovec {
                    iov_base: buf.as_mut_ptr() as *mut c_void,
                    iov_len: buf.len(),
                };
                let n = ffi::spdk_sock_readv(peer.sock, &mut iov, 1);
                if n > 0 {
                    peer.recv_buf.extend_from_slice(&buf[..n as usize]);
                    // Process complete messages from recv_buf.
                    loop {
                        if peer.recv_buf.len() < NDP_HEADER_SIZE {
                            break;
                        }
                        let header_arr: &[u8; NDP_HEADER_SIZE] =
                            match peer.recv_buf[..NDP_HEADER_SIZE].try_into() {
                                Ok(a) => a,
                                Err(_) => break,
                            };
                        let header = match NdpHeader::decode(header_arr) {
                            Ok(h) => h,
                            Err(_) => {
                                peer.recv_buf.clear();
                                break;
                            }
                        };
                        let total_msg_len = NDP_HEADER_SIZE + header.data_length as usize;
                        if peer.recv_buf.len() < total_msg_len {
                            break;
                        }
                        let data = if header.data_length > 0 {
                            Some(peer.recv_buf[NDP_HEADER_SIZE..total_msg_len].to_vec())
                        } else {
                            None
                        };
                        peer.recv_buf.drain(..total_msg_len);

                        // Match by request_id.
                        let resp_rid = header.request_id;
                        let req_idx = peer.pending.iter().position(|r| r.request_id == resp_rid);
                        if let Some(idx) = req_idx {
                            let req = peer.pending.remove(idx);
                            if header.status != 0 {
                                ffi::spdk_bdev_io_complete(req.bdev_io, -1i32);
                                continue;
                            }
                            match header.op {
                                NdpOp::ReadResp => {
                                    if let Some(ref data) = data {
                                        if let Some(ref iovs) = req.iovs {
                                            let mut src_off = req.buf_offset;
                                            for &(base, len) in iovs {
                                                let to_copy = std::cmp::min(
                                                    len,
                                                    data.len().saturating_sub(src_off),
                                                );
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
                                    ffi::spdk_bdev_io_complete(req.bdev_io, 0i32);
                                }
                                NdpOp::WriteResp => {
                                    ffi::spdk_bdev_io_complete(req.bdev_io, 0i32);
                                }
                                _ => {
                                    ffi::spdk_bdev_io_complete(req.bdev_io, -1i32);
                                }
                            }
                        }
                    }
                } else if n == 0 {
                    // EOF — peer closed.
                    read_dead.push(addr.clone());
                }
                // n < 0: EAGAIN, no data yet — that's fine.
            }
            for addr in read_dead {
                if let Some(mut dead) = state.peers.remove(&addr) {
                    ffi::spdk_sock_group_remove_sock(state.sock_group, dead.sock);
                    ffi::spdk_sock_close(&mut dead.sock);
                    for req in dead.pending {
                        ffi::spdk_bdev_io_complete(req.bdev_io, -1i32);
                    }
                    warn!("reactor_ndp: peer {} EOF during read poll", addr);
                }
            }
        }
    }

    rc.max(0)
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
            // EOF — peer closed connection. Remove from sock_group to stop
            // the poller from continuously firing on this dead socket.
            let cell = match REACTOR_NDP.get() {
                Some(c) => c,
                None => return,
            };
            let mut guard = cell.lock().unwrap();
            if let Some(state) = guard.as_mut() {
                // Remove socket from group first.
                ffi::spdk_sock_group_remove_sock(state.sock_group, sock);
                let mut sock_ptr = sock;
                ffi::spdk_sock_close(&mut sock_ptr);

                // Find and remove the peer entry.
                let dead_key: Option<String> = state
                    .peers
                    .iter()
                    .find(|(_, p)| std::ptr::eq(p.sock, sock))
                    .map(|(k, _)| k.clone());
                if let Some(key) = dead_key {
                    // Fail any pending requests on this peer.
                    if let Some(peer) = state.peers.remove(&key) {
                        for req in peer.pending {
                            ffi::spdk_bdev_io_complete(
                                req.bdev_io,
                                -1i32, /* SPDK_BDEV_IO_STATUS_FAILED */
                            );
                        }
                        warn!("reactor_ndp: peer {} disconnected, removed from pool", key);
                    }
                }
            }
        }
        // n < 0: EAGAIN or transient error — silently return, poller will retry.
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

        // Match response to pending request by request_id.
        // The NDP server processes requests concurrently, so responses
        // may arrive out of order. request_id ensures correct matching.
        let resp_rid = header.request_id;
        let req_idx = peer.pending.iter().position(|r| r.request_id == resp_rid);

        if let Some(idx) = req_idx {
            let req = peer.pending.remove(idx);
            let bdev_io = req.bdev_io;

            if header.status != 0 {
                ffi::spdk_bdev_io_complete(bdev_io, -1i32 /* FAILED */);
                continue;
            }

            match header.op {
                NdpOp::ReadResp => {
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
                    ffi::spdk_bdev_io_complete(bdev_io, 0i32 /* SUCCESS */);
                }
                NdpOp::WriteResp => {
                    ffi::spdk_bdev_io_complete(bdev_io, 0i32 /* SUCCESS */);
                }
                _ => {
                    ffi::spdk_bdev_io_complete(bdev_io, -1i32 /* FAILED */);
                }
            }
        } else {
            // No matching pending request — stale response, ignore.
            warn!(
                "reactor_ndp: unmatched response rid={} op={:?}",
                resp_rid, header.op
            );
        }
    }
}
