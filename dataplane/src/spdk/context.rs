//! SPDK thread dispatch utilities.
//!
//! SPDK operations must run on the SPDK app thread (reactor). When the
//! gRPC service receives a request on a tokio thread, it needs to
//! dispatch work to the SPDK reactor and wait for the result. This module
//! provides a [`Completion`] type for that synchronous request/response
//! pattern, and a [`dispatch_to_reactor`] function that sends a closure
//! to the SPDK reactor thread and blocks until it completes.

use std::sync::{Condvar, Mutex};

#[allow(
    non_camel_case_types,
    non_snake_case,
    non_upper_case_globals,
    dead_code,
    improper_ctypes
)]
mod ffi {
    include!(concat!(env!("OUT_DIR"), "/spdk_bindings.rs"));
}

/// A one-shot completion channel for synchronising an SPDK callback result
/// back to a waiting caller thread.
///
/// The caller creates a `Completion`, passes a raw pointer to the SPDK
/// callback via `as_ptr()`, and blocks on [`wait`]. The SPDK callback
/// recovers the `Completion` via [`from_ptr`] and signals the result.
pub struct Completion<T> {
    inner: Mutex<Option<T>>,
    cond: Condvar,
}

impl<T> Completion<T> {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(None),
            cond: Condvar::new(),
        }
    }

    /// Block until the completion is signalled and return the result.
    pub fn wait(&self) -> T {
        let mut guard = self.inner.lock().unwrap();
        while guard.is_none() {
            guard = self.cond.wait(guard).unwrap();
        }
        guard.take().unwrap()
    }

    /// Signal the completion with a value, waking the waiting thread.
    pub fn complete(&self, value: T) {
        let mut guard = self.inner.lock().unwrap();
        *guard = Some(value);
        self.cond.notify_one();
    }

    /// Convert to a raw pointer suitable for passing through SPDK's
    /// `void *cb_arg` parameter.
    pub fn as_ptr(&self) -> *mut std::os::raw::c_void {
        self as *const Self as *mut std::os::raw::c_void
    }

    /// Recover a reference from a raw pointer. The caller must guarantee
    /// that the pointer originated from [`as_ptr`] on a live `Completion`.
    ///
    /// # Safety
    /// The pointer must be valid and the `Completion` must still be alive.
    pub unsafe fn from_ptr<'a>(ptr: *mut std::os::raw::c_void) -> &'a Self {
        &*(ptr as *const Self)
    }
}

/// Dispatch a closure to the SPDK reactor (app) thread and block until it
/// completes, returning the result.
///
/// This is the primary mechanism for gRPC handlers (running on tokio threads)
/// to execute SPDK operations that must run on the reactor thread. The closure
/// runs inside a `spdk_thread_send_msg` callback on the app thread.
///
/// # Panics
/// Panics if the SPDK app thread is not available (SPDK not initialized).
pub fn dispatch_to_reactor<F, R>(f: F) -> R
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    // If we're already on an SPDK thread, execute directly.
    let on_spdk_thread = unsafe { !ffi::spdk_get_thread().is_null() };
    if on_spdk_thread {
        return f();
    }

    let completion = std::sync::Arc::new(Completion::<R>::new());
    let completion_clone = completion.clone();

    // Box the closure and completion into a context we can pass through void*.
    struct DispatchCtx<F, R> {
        func: Option<F>,
        completion: std::sync::Arc<Completion<R>>,
    }

    let ctx = Box::new(DispatchCtx {
        func: Some(f),
        completion: completion_clone,
    });
    let ctx_ptr = Box::into_raw(ctx) as *mut std::os::raw::c_void;

    unsafe extern "C" fn dispatch_cb<F, R>(arg: *mut std::os::raw::c_void)
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let mut ctx = Box::from_raw(arg as *mut DispatchCtx<F, R>);
        let func = ctx.func.take().unwrap();
        let result = func();
        ctx.completion.complete(result);
    }

    unsafe {
        let app_thread = ffi::spdk_thread_get_app_thread();
        assert!(!app_thread.is_null(), "SPDK app thread not available");

        let rc = ffi::spdk_thread_send_msg(app_thread, Some(dispatch_cb::<F, R>), ctx_ptr);
        assert!(rc == 0, "spdk_thread_send_msg failed: rc={}", rc);
    }

    completion.wait()
}

/// An async-compatible one-shot completion channel using tokio::sync::oneshot.
///
/// Unlike [`Completion`] (which blocks OS threads via Condvar), this can be
/// `.await`ed without blocking tokio worker threads.
pub struct AsyncCompletion<T> {
    rx: tokio::sync::oneshot::Receiver<T>,
}

/// The sender half of an async completion, passed to SPDK callbacks.
pub struct AsyncCompletionSender<T> {
    tx: Option<tokio::sync::oneshot::Sender<T>>,
}

impl<T> AsyncCompletion<T> {
    /// Create a new async completion pair (receiver, sender).
    pub fn new() -> (Self, AsyncCompletionSender<T>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (
            AsyncCompletion { rx },
            AsyncCompletionSender { tx: Some(tx) },
        )
    }

    /// Await the completion result.
    pub async fn wait(self) -> T {
        self.rx
            .await
            .expect("AsyncCompletion sender dropped without sending")
    }
}

impl<T> AsyncCompletionSender<T> {
    /// Signal the completion with a value.
    pub fn complete(&mut self, value: T) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(value);
        }
    }

    /// Convert to a raw pointer for passing through SPDK's `void *cb_arg`.
    pub fn into_ptr(self) -> *mut std::os::raw::c_void {
        Box::into_raw(Box::new(self)) as *mut std::os::raw::c_void
    }

    /// Recover from a raw pointer.
    ///
    /// # Safety
    /// The pointer must have been created by [`into_ptr`] and not yet consumed.
    pub unsafe fn from_ptr(ptr: *mut std::os::raw::c_void) -> Self {
        *Box::from_raw(ptr as *mut Self)
    }
}
