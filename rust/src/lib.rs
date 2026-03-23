/*!
MomentumX Rust bindings.

This crate wraps the MomentumX shared-memory IPC C++ library through a thin C
API layer (`mx_c_api.h`).  The public API mirrors the Python bindings: you
create a [`Context`], then open [`ProducerStream`] / [`ConsumerStream`] handles.

# Building

When used as a published crate, `build.rs` compiles the bundled C++ source
automatically via cmake — no manual steps required.

When building inside the MomentumX monorepo, run `make install` first so the
Python package (and its bundled `libmomentumx_static.a`) is available, then:

```sh
make rust/build
```

The Makefile sets `MOMENTUMX_BUILD_DIR` to the installed package's lib dir,
so cmake is not invoked a second time.

# Example — producer

```rust,no_run
use momentumx::{Context, LogLevel};
use std::time::Duration;

let ctx = Context::new("/dev/shm").expect("failed to create context");
ctx.set_log_level(LogLevel::Warning);

let stream = ctx
    .producer("my_stream", 1024 * 1024, 10, false)
    .expect("failed to create producer stream");

while stream.subscriber_count() == 0 {
    std::thread::sleep(Duration::from_millis(100));
}

if let Some(mut buf) = stream.next() {
    let len = {
        let data = buf.data_mut().unwrap();
        // write payload into data …
        data.len()
    };
    buf.send(len).unwrap();
}
```

# Example — consumer

```rust,no_run
use momentumx::Context;

let ctx = Context::new("/dev/shm").expect("failed to create context");
let stream = ctx.subscribe("my_stream").expect("failed to subscribe");

while stream.has_next() {
    if let Some(buf) = stream.receive(1) {
        let data = buf.data().unwrap();
        println!("received {} bytes, iter={}", data.len(), buf.iteration());
        // buf.release() is called automatically on drop
    }
}
```
*/

// ---------------------------------------------------------------------------
// Raw FFI declarations
// ---------------------------------------------------------------------------

pub mod ffi {
    use std::ffi::c_char;

    #[repr(C)]
    pub struct MxContext {
        _private: [u8; 0],
    }

    #[repr(C)]
    pub struct MxStream {
        _private: [u8; 0],
    }

    /// Mirror of `MxBufferState` in `mx_c_api.h`.
    #[repr(C)]
    #[derive(Debug, Clone, Copy, Default)]
    pub struct MxBufferState {
        pub buffer_id: u16,
        pub buffer_size: usize,
        pub buffer_count: usize,
        pub data_size: usize,
        pub data_timestamp: u64,
        pub iteration: u64,
    }

    #[repr(C)]
    #[derive(Debug, Clone, Copy)]
    pub enum MxLogLevel {
        Debug = 0,
        Info = 1,
        Warning = 2,
        Error = 3,
    }

    #[link(name = "momentumx_static", kind = "static")]
    unsafe extern "C" {
        pub fn mx_context_create(context_path: *const c_char) -> *mut MxContext;
        pub fn mx_context_destroy(ctx: *mut MxContext);
        pub fn mx_context_path(ctx: *mut MxContext) -> *const c_char;

        pub fn mx_stream_create(
            ctx: *mut MxContext,
            name: *const c_char,
            buffer_size: usize,
            buffer_count: usize,
            sync: bool,
        ) -> *mut MxStream;
        pub fn mx_stream_subscribe(ctx: *mut MxContext, name: *const c_char) -> *mut MxStream;
        pub fn mx_stream_unsubscribe(ctx: *mut MxContext, stream: *mut MxStream);

        pub fn mx_next(
            ctx: *mut MxContext,
            stream: *mut MxStream,
            out: *mut MxBufferState,
        ) -> i32;
        pub fn mx_send(
            ctx: *mut MxContext,
            stream: *mut MxStream,
            state: *const MxBufferState,
        ) -> i32;

        pub fn mx_can_receive(ctx: *mut MxContext, stream: *mut MxStream, min_ts: u64) -> bool;
        pub fn mx_receive(
            ctx: *mut MxContext,
            stream: *mut MxStream,
            min_ts: u64,
            out: *mut MxBufferState,
        ) -> i32;
        pub fn mx_release(
            ctx: *mut MxContext,
            stream: *mut MxStream,
            state: *const MxBufferState,
        );

        pub fn mx_data_address(
            ctx: *mut MxContext,
            stream: *mut MxStream,
            buffer_id: u16,
        ) -> *mut u8;

        // metadata
        pub fn mx_subscriber_count(ctx: *mut MxContext, stream: *mut MxStream) -> usize;
        pub fn mx_stream_buffer_size(ctx: *mut MxContext, stream: *mut MxStream) -> usize;
        pub fn mx_stream_buffer_count(ctx: *mut MxContext, stream: *mut MxStream) -> usize;
        pub fn mx_stream_is_sync(ctx: *mut MxContext, stream: *mut MxStream) -> bool;
        pub fn mx_stream_name(ctx: *mut MxContext, stream: *mut MxStream) -> *const c_char;

        // lifecycle
        pub fn mx_end(ctx: *mut MxContext, stream: *mut MxStream);
        pub fn mx_is_ended(ctx: *mut MxContext, stream: *mut MxStream) -> bool;
        pub fn mx_last_sent_iteration(ctx: *mut MxContext, stream: *mut MxStream) -> u64;

        pub fn mx_set_log_level(ctx: *mut MxContext, level: MxLogLevel);
    }
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors returned by MomentumX operations.
#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    /// The data_size supplied to `send` exceeds the buffer's allocated size.
    DataOverflow,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::DataOverflow => write!(f, "data size exceeds allocated buffer size"),
        }
    }
}

impl std::error::Error for Error {}

// ---------------------------------------------------------------------------
// Public safe types
// ---------------------------------------------------------------------------

use std::ffi::{CStr, CString};
use std::ptr::NonNull;

/// Logger verbosity level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Debug,
    Info,
    Warning,
    Error,
}

impl From<LogLevel> for ffi::MxLogLevel {
    fn from(l: LogLevel) -> Self {
        match l {
            LogLevel::Debug => ffi::MxLogLevel::Debug,
            LogLevel::Info => ffi::MxLogLevel::Info,
            LogLevel::Warning => ffi::MxLogLevel::Warning,
            LogLevel::Error => ffi::MxLogLevel::Error,
        }
    }
}

/// Metadata describing a buffer returned by [`Context::next_raw`] or
/// [`Context::receive_raw`].
#[derive(Debug, Clone, Copy)]
pub struct BufferState {
    pub buffer_id: u16,
    pub buffer_size: usize,
    pub buffer_count: usize,
    pub data_size: usize,
    pub data_timestamp: u64,
    pub iteration: u64,
}

impl From<ffi::MxBufferState> for BufferState {
    fn from(s: ffi::MxBufferState) -> Self {
        BufferState {
            buffer_id: s.buffer_id,
            buffer_size: s.buffer_size,
            buffer_count: s.buffer_count,
            data_size: s.data_size,
            data_timestamp: s.data_timestamp,
            iteration: s.iteration,
        }
    }
}

impl From<BufferState> for ffi::MxBufferState {
    fn from(s: BufferState) -> Self {
        ffi::MxBufferState {
            buffer_id: s.buffer_id,
            buffer_size: s.buffer_size,
            buffer_count: s.buffer_count,
            data_size: s.data_size,
            data_timestamp: s.data_timestamp,
            iteration: s.iteration,
        }
    }
}

/// A raw handle to an open stream (producer or consumer).
///
/// Non-owning: freed automatically when the owning [`ProducerStream`] or
/// [`ConsumerStream`] wrapper is dropped.
pub struct StreamHandle(NonNull<ffi::MxStream>);

unsafe impl Send for StreamHandle {}
unsafe impl Sync for StreamHandle {}

// ---------------------------------------------------------------------------
// RAII stream wrappers
// ---------------------------------------------------------------------------

/// RAII producer stream handle.  Freed on drop.
pub struct ProducerStream<'ctx> {
    ctx: &'ctx Context,
    handle: StreamHandle,
}

impl<'ctx> ProducerStream<'ctx> {
    /// Get the next writable buffer (non-blocking). Returns `None` if no
    /// buffer is currently available.
    pub fn next(&self) -> Option<SendBuffer<'_>> {
        self.ctx.next_raw(&self.handle).map(|s| SendBuffer {
            state: s,
            stream: self,
        })
    }

    pub fn subscriber_count(&self) -> usize {
        unsafe { ffi::mx_subscriber_count(self.ctx.ptr.as_ptr(), self.handle.0.as_ptr()) }
    }

    pub fn buffer_size(&self) -> usize {
        unsafe { ffi::mx_stream_buffer_size(self.ctx.ptr.as_ptr(), self.handle.0.as_ptr()) }
    }

    pub fn buffer_count(&self) -> usize {
        unsafe { ffi::mx_stream_buffer_count(self.ctx.ptr.as_ptr(), self.handle.0.as_ptr()) }
    }

    pub fn is_sync(&self) -> bool {
        unsafe { ffi::mx_stream_is_sync(self.ctx.ptr.as_ptr(), self.handle.0.as_ptr()) }
    }

    pub fn name(&self) -> &str {
        let ptr =
            unsafe { ffi::mx_stream_name(self.ctx.ptr.as_ptr(), self.handle.0.as_ptr()) };
        if ptr.is_null() {
            return "";
        }
        unsafe { CStr::from_ptr(ptr) }.to_str().unwrap_or("")
    }

    pub fn is_ended(&self) -> bool {
        unsafe { ffi::mx_is_ended(self.ctx.ptr.as_ptr(), self.handle.0.as_ptr()) }
    }

    /// Mark the stream as ended. Consumers will stop blocking once they have
    /// read all buffered data.
    pub fn end(&self) {
        unsafe { ffi::mx_end(self.ctx.ptr.as_ptr(), self.handle.0.as_ptr()) };
    }

    pub fn last_sent_iteration(&self) -> u64 {
        unsafe { ffi::mx_last_sent_iteration(self.ctx.ptr.as_ptr(), self.handle.0.as_ptr()) }
    }

    pub fn raw_handle(&self) -> &StreamHandle {
        &self.handle
    }
}

impl Drop for ProducerStream<'_> {
    fn drop(&mut self) {
        unsafe {
            ffi::mx_stream_unsubscribe(self.ctx.ptr.as_ptr(), self.handle.0.as_ptr());
        }
    }
}

/// RAII consumer stream handle.  Freed on drop.
pub struct ConsumerStream<'ctx> {
    ctx: &'ctx Context,
    handle: StreamHandle,
}

impl<'ctx> ConsumerStream<'ctx> {
    /// Returns `true` if a buffer with timestamp > `min_ts` is available.
    pub fn can_receive(&self, min_ts: u64) -> bool {
        unsafe { ffi::mx_can_receive(self.ctx.ptr.as_ptr(), self.handle.0.as_ptr(), min_ts) }
    }

    /// Equivalent to `can_receive(1)` — true when any unread data exists.
    pub fn has_next(&self) -> bool {
        self.can_receive(1)
    }

    /// Receive the next buffer (non-blocking). Returns `None` if none is
    /// currently available.
    pub fn receive(&self, min_ts: u64) -> Option<RecvBuffer<'_>> {
        self.ctx.receive_raw(&self.handle, min_ts).map(|s| RecvBuffer {
            state: s,
            stream: self,
            released: false,
        })
    }

    pub fn subscriber_count(&self) -> usize {
        unsafe { ffi::mx_subscriber_count(self.ctx.ptr.as_ptr(), self.handle.0.as_ptr()) }
    }

    pub fn buffer_size(&self) -> usize {
        unsafe { ffi::mx_stream_buffer_size(self.ctx.ptr.as_ptr(), self.handle.0.as_ptr()) }
    }

    pub fn buffer_count(&self) -> usize {
        unsafe { ffi::mx_stream_buffer_count(self.ctx.ptr.as_ptr(), self.handle.0.as_ptr()) }
    }

    pub fn is_sync(&self) -> bool {
        unsafe { ffi::mx_stream_is_sync(self.ctx.ptr.as_ptr(), self.handle.0.as_ptr()) }
    }

    pub fn name(&self) -> &str {
        let ptr =
            unsafe { ffi::mx_stream_name(self.ctx.ptr.as_ptr(), self.handle.0.as_ptr()) };
        if ptr.is_null() {
            return "";
        }
        unsafe { CStr::from_ptr(ptr) }.to_str().unwrap_or("")
    }

    pub fn is_ended(&self) -> bool {
        unsafe { ffi::mx_is_ended(self.ctx.ptr.as_ptr(), self.handle.0.as_ptr()) }
    }

    pub fn last_sent_iteration(&self) -> u64 {
        unsafe { ffi::mx_last_sent_iteration(self.ctx.ptr.as_ptr(), self.handle.0.as_ptr()) }
    }

    pub fn raw_handle(&self) -> &StreamHandle {
        &self.handle
    }
}

impl Drop for ConsumerStream<'_> {
    fn drop(&mut self) {
        unsafe {
            ffi::mx_stream_unsubscribe(self.ctx.ptr.as_ptr(), self.handle.0.as_ptr());
        }
    }
}

// ---------------------------------------------------------------------------
// Buffer wrappers
// ---------------------------------------------------------------------------

/// A writable buffer obtained from [`ProducerStream::next`].
///
/// Call [`SendBuffer::send`] to publish. If dropped without calling `send`,
/// the write claim is held until the next call to [`ProducerStream::next`]
/// or until the stream itself is dropped — it is not immediately returned
/// to the pool. Prefer calling `send` (or explicitly dropping the stream)
/// rather than relying on deferred release.
pub struct SendBuffer<'s> {
    pub state: BufferState,
    stream: &'s ProducerStream<'s>,
}

impl<'s> SendBuffer<'s> {
    pub fn buffer_id(&self) -> u16 {
        self.state.buffer_id
    }

    pub fn buffer_size(&self) -> usize {
        self.state.buffer_size
    }

    /// Mutable view of the full shared-memory region for this buffer
    /// (`buffer_size` bytes).
    pub fn data_mut(&mut self) -> Option<&mut [u8]> {
        let ptr = unsafe {
            ffi::mx_data_address(
                self.stream.ctx.ptr.as_ptr(),
                self.stream.handle.0.as_ptr(),
                self.state.buffer_id,
            )
        };
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { std::slice::from_raw_parts_mut(ptr, self.state.buffer_size) })
        }
    }

    /// Publish the buffer with `data_size` valid bytes.
    ///
    /// Returns `Err(Error::DataOverflow)` if `data_size > buffer_size`.
    pub fn send(mut self, data_size: usize) -> Result<bool, Error> {
        if data_size > self.state.buffer_size {
            return Err(Error::DataOverflow);
        }
        self.state.data_size = data_size;
        let raw: ffi::MxBufferState = self.state.into();
        let result = unsafe {
            ffi::mx_send(
                self.stream.ctx.ptr.as_ptr(),
                self.stream.handle.0.as_ptr(),
                &raw,
            )
        };
        Ok(result == 1)
    }
}

/// A received buffer obtained from [`ConsumerStream::receive`].
///
/// Automatically released on drop.
pub struct RecvBuffer<'s> {
    pub state: BufferState,
    stream: &'s ConsumerStream<'s>,
    released: bool,
}

impl<'s> RecvBuffer<'s> {
    pub fn buffer_id(&self) -> u16 {
        self.state.buffer_id
    }

    pub fn data_size(&self) -> usize {
        self.state.data_size
    }

    pub fn iteration(&self) -> u64 {
        self.state.iteration
    }

    pub fn timestamp(&self) -> u64 {
        self.state.data_timestamp
    }

    /// Read-only view of the received data (`data_size` bytes).
    pub fn data(&self) -> Option<&[u8]> {
        let ptr = unsafe {
            ffi::mx_data_address(
                self.stream.ctx.ptr.as_ptr(),
                self.stream.handle.0.as_ptr(),
                self.state.buffer_id,
            )
        };
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { std::slice::from_raw_parts(ptr, self.state.data_size) })
        }
    }

    /// Full shared-memory region (buffer_size bytes, read-only).
    pub fn data_full(&self) -> Option<&[u8]> {
        let ptr = unsafe {
            ffi::mx_data_address(
                self.stream.ctx.ptr.as_ptr(),
                self.stream.handle.0.as_ptr(),
                self.state.buffer_id,
            )
        };
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { std::slice::from_raw_parts(ptr, self.state.buffer_size) })
        }
    }

    /// Explicitly release this buffer before drop.
    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if !self.released {
            let raw: ffi::MxBufferState = self.state.into();
            unsafe {
                ffi::mx_release(
                    self.stream.ctx.ptr.as_ptr(),
                    self.stream.handle.0.as_ptr(),
                    &raw,
                );
            }
            self.released = true;
        }
    }
}

impl Drop for RecvBuffer<'_> {
    fn drop(&mut self) {
        self.release_inner();
    }
}

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

/// Entry point for all MomentumX operations.
///
/// Corresponds to a directory on the filesystem (usually `/dev/shm`) where
/// stream control files and shared-memory buffers live.
///
/// **Important:** each producer and each consumer must use its own `Context`
/// instance, mirroring how the Python bindings work internally.
pub struct Context {
    ptr: NonNull<ffi::MxContext>,
}

unsafe impl Send for Context {}
unsafe impl Sync for Context {}

impl Context {
    /// Create a new context backed by `context_path` (e.g. `"/dev/shm"`).
    /// Returns `None` if creation fails.
    pub fn new(context_path: &str) -> Option<Self> {
        let cstr = CString::new(context_path).ok()?;
        let ptr = unsafe { ffi::mx_context_create(cstr.as_ptr()) };
        NonNull::new(ptr).map(|ptr| Context { ptr })
    }

    /// The path this context was opened with.
    pub fn context_path(&self) -> &str {
        let ptr = unsafe { ffi::mx_context_path(self.ptr.as_ptr()) };
        if ptr.is_null() {
            return "";
        }
        unsafe { CStr::from_ptr(ptr) }.to_str().unwrap_or("")
    }

    pub fn set_log_level(&self, level: LogLevel) {
        unsafe { ffi::mx_set_log_level(self.ptr.as_ptr(), level.into()) };
    }

    // --- RAII stream constructors ---

    /// Open a producer (writer) stream.
    ///
    /// Returns `None` if a producer stream with this name already exists.
    pub fn producer<'ctx>(
        &'ctx self,
        name: &str,
        buffer_size: usize,
        buffer_count: usize,
        sync: bool,
    ) -> Option<ProducerStream<'ctx>> {
        let handle = self.create_stream_raw(name, buffer_size, buffer_count, sync)?;
        Some(ProducerStream { ctx: self, handle })
    }

    /// Subscribe to an existing stream as a consumer.
    ///
    /// Returns `None` if the stream does not exist.
    pub fn subscribe<'ctx>(&'ctx self, name: &str) -> Option<ConsumerStream<'ctx>> {
        let handle = self.subscribe_raw(name)?;
        Some(ConsumerStream { ctx: self, handle })
    }

    // --- Low-level (raw handle) API ---

    pub fn create_stream_raw(
        &self,
        name: &str,
        buffer_size: usize,
        buffer_count: usize,
        sync: bool,
    ) -> Option<StreamHandle> {
        let cstr = CString::new(name).ok()?;
        let ptr = unsafe {
            ffi::mx_stream_create(
                self.ptr.as_ptr(),
                cstr.as_ptr(),
                buffer_size,
                buffer_count,
                sync,
            )
        };
        NonNull::new(ptr).map(StreamHandle)
    }

    pub fn subscribe_raw(&self, name: &str) -> Option<StreamHandle> {
        let cstr = CString::new(name).ok()?;
        let ptr = unsafe { ffi::mx_stream_subscribe(self.ptr.as_ptr(), cstr.as_ptr()) };
        NonNull::new(ptr).map(StreamHandle)
    }

    pub fn unsubscribe(&self, stream: StreamHandle) {
        unsafe { ffi::mx_stream_unsubscribe(self.ptr.as_ptr(), stream.0.as_ptr()) };
    }

    /// Get the next writable buffer (non-blocking).
    pub fn next_raw(&self, stream: &StreamHandle) -> Option<BufferState> {
        let mut out = ffi::MxBufferState::default();
        let r = unsafe { ffi::mx_next(self.ptr.as_ptr(), stream.0.as_ptr(), &mut out) };
        if r == 1 { Some(out.into()) } else { None }
    }

    /// Send a buffer. Returns `true` on success.
    pub fn send_raw(&self, stream: &StreamHandle, state: BufferState) -> bool {
        let raw: ffi::MxBufferState = state.into();
        unsafe { ffi::mx_send(self.ptr.as_ptr(), stream.0.as_ptr(), &raw) == 1 }
    }

    pub fn can_receive_raw(&self, stream: &StreamHandle, min_ts: u64) -> bool {
        unsafe { ffi::mx_can_receive(self.ptr.as_ptr(), stream.0.as_ptr(), min_ts) }
    }

    /// Receive the next buffer (non-blocking).
    pub fn receive_raw(&self, stream: &StreamHandle, min_ts: u64) -> Option<BufferState> {
        let mut out = ffi::MxBufferState::default();
        let r =
            unsafe { ffi::mx_receive(self.ptr.as_ptr(), stream.0.as_ptr(), min_ts, &mut out) };
        if r == 1 { Some(out.into()) } else { None }
    }

    pub fn release_raw(&self, stream: &StreamHandle, state: BufferState) {
        let raw: ffi::MxBufferState = state.into();
        unsafe { ffi::mx_release(self.ptr.as_ptr(), stream.0.as_ptr(), &raw) };
    }

    /// # Safety
    /// Valid while the buffer is checked out (between next/receive and
    /// send/release).
    pub unsafe fn data_address(&self, stream: &StreamHandle, buffer_id: u16) -> *mut u8 {
        ffi::mx_data_address(self.ptr.as_ptr(), stream.0.as_ptr(), buffer_id)
    }

    pub fn subscriber_count_raw(&self, stream: &StreamHandle) -> usize {
        unsafe { ffi::mx_subscriber_count(self.ptr.as_ptr(), stream.0.as_ptr()) }
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        unsafe { ffi::mx_context_destroy(self.ptr.as_ptr()) };
    }
}

// ---------------------------------------------------------------------------
// Tests  (mirror of tests/test_scenarios.py)
// ---------------------------------------------------------------------------
//
// KEY RULE: every producer and every consumer must use its own Context,
// exactly as the Python binding does internally.  Sharing a Context between a
// producer and a consumer causes subscribe() to return the same underlying
// stream pointer, so subscriber_count stays 0 and receives see no data.

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    static NAME_CTR: AtomicU64 = AtomicU64::new(0);
    const CTX: &str = "/dev/shm";
    const T: Duration = Duration::from_secs(5);

    fn uname() -> String {
        let n = NAME_CTR.fetch_add(1, Ordering::Relaxed);
        format!("t{n}")
    }

    /// Remove leftover /dev/shm files for a stream created by this test suite.
    fn cleanup(ctx_path: &str, stream_name: &str) {
        let prefix = format!("{ctx_path}/mx.{stream_name}");
        if let Ok(entries) = std::fs::read_dir(ctx_path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path
                    .file_name()
                    .and_then(|f| f.to_str())
                    .map(|f| f.starts_with(&format!("mx.{stream_name}")))
                    .unwrap_or(false)
                {
                    assert!(
                        prefix.starts_with("/dev/shm/mx.t")
                            || prefix.starts_with("/tmp/mx.t"),
                        "refusing to delete {prefix}"
                    );
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
    }

    fn next_blocking<'s>(stream: &'s ProducerStream<'s>, timeout: Duration) -> Option<SendBuffer<'s>> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(b) = stream.next() {
                return Some(b);
            }
            if Instant::now() >= deadline {
                return None;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn recv_blocking<'s>(
        stream: &'s ConsumerStream<'s>,
        min_ts: u64,
        timeout: Duration,
    ) -> Option<RecvBuffer<'s>> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(b) = stream.receive(min_ts) {
                return Some(b);
            }
            if Instant::now() >= deadline {
                return None;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    // -----------------------------------------------------------------------
    // test_subscribers
    // -----------------------------------------------------------------------
    #[test]
    fn test_subscribers() {
        let name = uname();
        cleanup(CTX, &name);

        let prod_ctx = Context::new(CTX).unwrap();
        let producer = prod_ctx.producer(&name, 20, 5, true).unwrap();
        assert_eq!(producer.subscriber_count(), 0, "no subscribers yet");

        let c1_ctx = Context::new(CTX).unwrap();
        let consumer1 = c1_ctx.subscribe(&name).unwrap();
        assert_eq!(producer.subscriber_count(), 1, "one subscriber");

        let c2_ctx = Context::new(CTX).unwrap();
        let _consumer2 = c2_ctx.subscribe(&name).unwrap();
        drop(consumer1);
        assert_eq!(producer.subscriber_count(), 1, "back to one after drop");

        drop(_consumer2);
        assert_eq!(producer.subscriber_count(), 0, "zero after both dropped");
    }

    // -----------------------------------------------------------------------
    // test_stream_unavailable_exception
    // -----------------------------------------------------------------------
    #[test]
    fn test_stream_unavailable() {
        let ctx = Context::new(CTX).unwrap();
        assert!(ctx.subscribe("__nonexistent_stream__").is_none());
    }

    // -----------------------------------------------------------------------
    // test_exception_on_duplicate_stream
    // -----------------------------------------------------------------------
    #[test]
    fn test_duplicate_stream() {
        let name = uname();
        cleanup(CTX, &name);

        let ctx = Context::new(CTX).unwrap();
        let _p1 = ctx.producer(&name, 4096, 1, false).unwrap();

        // second producer on a different context — still duplicate
        let ctx2 = Context::new(CTX).unwrap();
        assert!(ctx2.producer(&name, 4096, 1, false).is_none());
    }

    // -----------------------------------------------------------------------
    // test_buffer_overflow_exception / test_string_overflow_exception
    // -----------------------------------------------------------------------
    #[test]
    fn test_data_overflow_send() {
        let name = uname();
        cleanup(CTX, &name);

        let ctx = Context::new(CTX).unwrap();
        let prod = ctx.producer(&name, 4096, 1, false).unwrap();
        let buf = prod.next().unwrap();
        assert_eq!(buf.send(4097), Err(Error::DataOverflow));
    }

    // -----------------------------------------------------------------------
    // test_send_data_size_equal_to_buffer_size_explicit/implicit
    // -----------------------------------------------------------------------
    #[test]
    fn test_send_data_size_equal_to_buffer_size() {
        let name = uname();
        cleanup(CTX, &name);
        let size: usize = 5;

        let ctx = Context::new(CTX).unwrap();
        let prod = ctx.producer(&name, size, 1, false).unwrap();

        // explicit: send(size) must NOT overflow
        let buf = prod.next().unwrap();
        assert_eq!(buf.buffer_size(), size);
        buf.send(size).unwrap();

        // implicit: fill all bytes then send
        let mut buf2 = next_blocking(&prod, T).unwrap();
        let written = {
            let data = buf2.data_mut().unwrap();
            data.fill(0xff);
            data.len()
        };
        assert_eq!(written, size);
        buf2.send(written).unwrap();
    }

    // -----------------------------------------------------------------------
    // test_less_than_page_size
    // -----------------------------------------------------------------------
    #[test]
    fn test_less_than_page_size() {
        let name = uname();
        cleanup(CTX, &name);

        let ctx = Context::new(CTX).unwrap();
        let prod = ctx.producer(&name, 1, 1, false).unwrap();
        let buf = prod.next().unwrap();
        assert_eq!(buf.buffer_size(), 1);
    }

    // -----------------------------------------------------------------------
    // test_empty_slice_matches_buffer_length_producer
    // -----------------------------------------------------------------------
    #[test]
    fn test_buffer_full_size_accessible() {
        let name = uname();
        cleanup(CTX, &name);

        let ctx = Context::new(CTX).unwrap();
        let prod = ctx.producer(&name, 4096, 1, false).unwrap();
        let mut buf = prod.next().unwrap();
        assert_eq!(buf.data_mut().unwrap().len(), 4096);
    }

    // -----------------------------------------------------------------------
    // test_buffer_file_api_producer/consumer + test_buffer_read_indexing_operator
    // -----------------------------------------------------------------------
    #[test]
    fn test_buffer_read_write() {
        let name = uname();
        cleanup(CTX, &name);

        let prod_ctx = Context::new(CTX).unwrap();
        let cons_ctx = Context::new(CTX).unwrap();
        let prod = prod_ctx.producer(&name, 4096, 1, false).unwrap();
        let cons = cons_ctx.subscribe(&name).unwrap();

        let payload = b"foobar";
        {
            let mut buf = prod.next().unwrap();
            buf.data_mut().unwrap()[..payload.len()].copy_from_slice(payload);
            buf.send(payload.len()).unwrap();
        }

        let rx = recv_blocking(&cons, 1, T).unwrap();
        assert_eq!(rx.data().unwrap(), payload);
        assert_eq!(rx.data_size(), payload.len());
    }

    // -----------------------------------------------------------------------
    // test_buffer_read_indexing_operator_correct_send_size
    // -----------------------------------------------------------------------
    #[test]
    fn test_correct_data_size_on_receive() {
        let name = uname();
        cleanup(CTX, &name);

        let prod_ctx = Context::new(CTX).unwrap();
        let cons_ctx = Context::new(CTX).unwrap();
        let prod = prod_ctx.producer(&name, 4096, 1, false).unwrap();
        let cons = cons_ctx.subscribe(&name).unwrap();

        {
            let mut buf = prod.next().unwrap();
            buf.data_mut().unwrap()[2] = 97; // 'a'
            buf.send(3).unwrap();
        }

        let rx = recv_blocking(&cons, 1, T).unwrap();
        assert_eq!(rx.data_size(), 3);
        assert_eq!(rx.data().unwrap(), b"\x00\x00a");
    }

    // -----------------------------------------------------------------------
    // test_buffer_cleanup
    // -----------------------------------------------------------------------
    #[test]
    fn test_buffer_cleanup() {
        let name = uname();
        cleanup(CTX, &name);

        let buffer_file = format!("{CTX}/mx.{name}.buffer.1");
        {
            let ctx = Context::new(CTX).unwrap();
            let prod = ctx.producer(&name, 300, 20, true).unwrap();
            assert!(prod.is_sync());
            assert_eq!(prod.buffer_size(), 300);
            assert_eq!(prod.buffer_count(), 20);
            assert!(prod.name().contains(&name));
            assert!(std::path::Path::new(&buffer_file).exists());
        }
        assert!(!std::path::Path::new(&buffer_file).exists());
    }

    // -----------------------------------------------------------------------
    // test_buffer_cleanup_tmp
    // -----------------------------------------------------------------------
    #[test]
    fn test_buffer_cleanup_custom_path() {
        let name = uname();
        let tmp = std::env::temp_dir();
        let tmp_path = tmp.to_str().unwrap();
        cleanup(CTX, &name);
        cleanup(tmp_path, &name);

        let shm_ctrl = format!("{CTX}/mx.{name}");
        let shm_buf = format!("{CTX}/mx.{name}.buffer.1");
        let tmp_ctrl = format!("{tmp_path}/mx.{name}");
        let tmp_buf = format!("{tmp_path}/mx.{name}.buffer.1");

        {
            let ctx = Context::new(CTX).unwrap();
            let _prod = ctx.producer(&name, 300, 20, true).unwrap();
            assert!(std::path::Path::new(&shm_ctrl).exists());
            assert!(std::path::Path::new(&shm_buf).exists());
            assert!(!std::path::Path::new(&tmp_ctrl).exists());
        }
        assert!(!std::path::Path::new(&shm_ctrl).exists());
        assert!(!std::path::Path::new(&shm_buf).exists());

        {
            let ctx = Context::new(tmp_path).unwrap();
            let _prod = ctx.producer(&name, 300, 20, true).unwrap();
            assert!(!std::path::Path::new(&shm_ctrl).exists());
            assert!(std::path::Path::new(&tmp_ctrl).exists());
            assert!(std::path::Path::new(&tmp_buf).exists());
        }
        assert!(!std::path::Path::new(&tmp_ctrl).exists());
        assert!(!std::path::Path::new(&tmp_buf).exists());
    }

    // -----------------------------------------------------------------------
    // test_synced_buffers
    // -----------------------------------------------------------------------
    #[test]
    fn test_synced_buffers() {
        let name = uname();
        cleanup(CTX, &name);
        let buffer_count: usize = 5;

        let prod_ctx = Context::new(CTX).unwrap();
        let cons_ctx = Context::new(CTX).unwrap();
        let prod = prod_ctx.producer(&name, 1, buffer_count, true).unwrap();
        let cons = cons_ctx.subscribe(&name).unwrap();

        for n in 1u8..=5 {
            let expected_id = (n as usize % buffer_count) as u16;
            let mut tx = next_blocking(&prod, T).expect("next timed out");
            assert_eq!(tx.buffer_id(), expected_id);
            tx.data_mut().unwrap()[0] = n;
            tx.send(1).unwrap();

            let rx = recv_blocking(&cons, 1, T).expect("receive timed out");
            assert_eq!(rx.data().unwrap()[0], n);
            rx.release();
        }
    }

    // -----------------------------------------------------------------------
    // test_durable_ending
    // -----------------------------------------------------------------------
    #[test]
    fn test_durable_ending() {
        let name = uname();
        cleanup(CTX, &name);
        let buffer_count = 27usize;

        let prod_ctx = Context::new(CTX).unwrap();
        let cons_ctx = Context::new(CTX).unwrap();
        let prod = prod_ctx.producer(&name, 1, buffer_count, true).unwrap();
        let cons = cons_ctx.subscribe(&name).unwrap();

        for n in 1u8..=(buffer_count as u8) {
            let mut b = next_blocking(&prod, T).unwrap();
            b.data_mut().unwrap()[0] = n;
            b.send(1).unwrap();
        }

        assert!(!prod.is_ended());
        prod.end();
        assert!(prod.is_ended());

        for n in 1u8..=(buffer_count as u8) {
            assert!(cons.has_next(), "should have_next for n={n}");
            let rx = recv_blocking(&cons, 1, T).unwrap();
            assert_eq!(rx.data().unwrap()[0], n);
        }
        assert!(!cons.has_next());
    }

    // -----------------------------------------------------------------------
    // test_synced_buffers_many_read_after_many_write
    // -----------------------------------------------------------------------
    #[test]
    fn test_synced_buffers_many_write_then_read() {
        let name = uname();
        cleanup(CTX, &name);
        let buffer_count = 5usize;

        let prod_ctx = Context::new(CTX).unwrap();
        let cons_ctx = Context::new(CTX).unwrap();
        let prod = prod_ctx.producer(&name, 1, buffer_count, true).unwrap();
        let cons = cons_ctx.subscribe(&name).unwrap();

        for n in 1u8..=(buffer_count as u8) {
            let expected_id = (n as usize % buffer_count) as u16;
            let mut tx = next_blocking(&prod, T).unwrap();
            assert_eq!(tx.buffer_id(), expected_id);
            tx.data_mut().unwrap()[0] = n;
            tx.send(1).unwrap();
        }

        // All buffers sent and unacknowledged — next write must return None
        assert!(prod.next().is_none(), "expected None after writing all unacknowledged buffers");

        for n in 1u8..=(buffer_count as u8) {
            let expected_id = (n as usize % buffer_count) as u16;
            let rx = recv_blocking(&cons, 1, T).unwrap();
            assert_eq!(rx.buffer_id(), expected_id);
            assert_eq!(rx.data().unwrap()[0], n);
            rx.release();
        }
    }

    // -----------------------------------------------------------------------
    // test_two_consumer_copies
    // -----------------------------------------------------------------------
    #[test]
    fn test_two_consumers() {
        let name = uname();
        cleanup(CTX, &name);

        let prod_ctx = Context::new(CTX).unwrap();
        let c1_ctx = Context::new(CTX).unwrap();
        let c2_ctx = Context::new(CTX).unwrap();
        let prod = prod_ctx.producer(&name, 20, 2, true).unwrap();
        let cons1 = c1_ctx.subscribe(&name).unwrap();
        let cons2 = c2_ctx.subscribe(&name).unwrap();

        for idx in 0u8..3 {
            let payload = [idx];
            let mut buf = next_blocking(&prod, T).unwrap();
            buf.data_mut().unwrap()[..1].copy_from_slice(&payload);
            buf.send(1).unwrap();

            let r1 = recv_blocking(&cons1, 1, T).unwrap();
            assert_eq!(r1.data().unwrap(), &payload);

            let r2 = recv_blocking(&cons2, 1, T).unwrap();
            assert_eq!(r2.data().unwrap(), &payload);
        }
    }

    // -----------------------------------------------------------------------
    // test_implicit_release  (threaded; separate contexts inside threads)
    // -----------------------------------------------------------------------
    #[test]
    fn test_implicit_release_threaded() {
        let name = uname();
        cleanup(CTX, &name);
        let buffer_count = 3usize;
        let send_count = buffer_count * 2;
        let name = Arc::new(name);

        let pname = Arc::clone(&name);
        let prod_handle = thread::spawn(move || {
            let ctx = Context::new(CTX).unwrap();
            let prod = ctx.producer(&*pname, 1, buffer_count, true).unwrap();
            let dl = Instant::now() + T;
            while prod.subscriber_count() == 0 {
                assert!(Instant::now() < dl, "timed out waiting for subscriber");
                thread::sleep(Duration::from_millis(10));
            }
            let mut sent = 0;
            for _ in 0..send_count {
                if let Some(buf) = next_blocking(&prod, T) {
                    buf.send(1).unwrap();
                    sent += 1;
                }
            }
            sent
        });

        let cname = Arc::clone(&name);
        let cons_handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            let ctx = Context::new(CTX).unwrap();
            let cons = loop {
                if let Some(c) = ctx.subscribe(&*cname) { break c; }
                thread::sleep(Duration::from_millis(10));
            };
            let mut received = 0;
            while cons.has_next() {
                if recv_blocking(&cons, 1, T).is_some() {
                    received += 1;
                }
            }
            received
        });

        assert_eq!(prod_handle.join().unwrap(), send_count);
        assert_eq!(cons_handle.join().unwrap(), send_count);
    }

    // -----------------------------------------------------------------------
    // test_durable_receives  (threaded)
    // -----------------------------------------------------------------------
    #[test]
    fn test_durable_receives_threaded() {
        let name = uname();
        cleanup(CTX, &name);
        let buffer_count = 3usize;
        let name = Arc::new(name);

        let pname = Arc::clone(&name);
        let prod_handle = thread::spawn(move || {
            let ctx = Context::new(CTX).unwrap();
            let prod = ctx.producer(&*pname, 1, buffer_count, true).unwrap();
            let dl = Instant::now() + T;
            while prod.subscriber_count() == 0 {
                assert!(Instant::now() < dl, "timed out waiting for subscriber");
                thread::sleep(Duration::from_millis(10));
            }
            let mut sent = 0;
            for _ in 0..buffer_count {
                if let Some(buf) = next_blocking(&prod, T) {
                    buf.send(1).unwrap();
                    sent += 1;
                }
            }
            sent
        });

        let cname = Arc::clone(&name);
        let cons_handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            let ctx = Context::new(CTX).unwrap();
            let cons = loop {
                if let Some(c) = ctx.subscribe(&*cname) { break c; }
                thread::sleep(Duration::from_millis(10));
            };
            let mut received = 0;
            while cons.has_next() {
                if recv_blocking(&cons, 1, T).is_some() {
                    received += 1;
                    thread::sleep(Duration::from_millis(100));
                }
            }
            received
        });

        assert_eq!(prod_handle.join().unwrap(), buffer_count);
        assert_eq!(cons_handle.join().unwrap(), buffer_count);
    }

    // -----------------------------------------------------------------------
    // test_grab_oldest
    // -----------------------------------------------------------------------
    #[test]
    fn test_grab_oldest() {
        let name = uname();
        cleanup(CTX, &name);

        let prod_ctx = Context::new(CTX).unwrap();
        let cons_ctx = Context::new(CTX).unwrap();
        let prod = prod_ctx.producer(&name, 20, 5, false).unwrap();
        let cons = cons_ctx.subscribe(&name).unwrap();

        // helper: write one byte and send
        let push = |val: u8| {
            let mut buf = next_blocking(&prod, T).unwrap();
            buf.data_mut().unwrap()[0] = val;
            buf.send(1).unwrap();
        };

        push(0); push(0);
        assert_eq!(recv_blocking(&cons, 1, T).unwrap().buffer_id(), 1);
        assert_eq!(recv_blocking(&cons, 1, T).unwrap().buffer_id(), 2);
        assert_eq!(cons.last_sent_iteration(), 2);
        assert_eq!(prod.last_sent_iteration(), 2);

        for v in [10u8, 20, 30, 40, 50] { push(v); }
        let b3 = recv_blocking(&cons, 1, T).unwrap();
        let b4 = recv_blocking(&cons, 1, T).unwrap();
        let b5 = recv_blocking(&cons, 1, T).unwrap();
        let b1 = recv_blocking(&cons, 1, T).unwrap();
        let b2 = recv_blocking(&cons, 1, T).unwrap();

        assert_eq!(b3.buffer_id(), 3);
        assert_eq!(b4.buffer_id(), 4);
        assert_eq!(b5.buffer_id(), 5 % 5);
        assert_eq!(b1.buffer_id(), 1);
        assert_eq!(b2.buffer_id(), 2);
        assert_eq!(b3.data().unwrap()[0], 10);
        assert_eq!(b4.data().unwrap()[0], 20);
        assert_eq!(b5.data().unwrap()[0], 30);
        assert_eq!(b1.data().unwrap()[0], 40);
        assert_eq!(b2.data().unwrap()[0], 50);

        drop(b5); drop(b3); drop(b2); drop(b1); // release all except b4

        for v in [60u8, 70, 80, 90] { push(v); }
        let nb3 = recv_blocking(&cons, 1, T).unwrap();
        let nb5 = recv_blocking(&cons, 1, T).unwrap();
        let nb1 = recv_blocking(&cons, 1, T).unwrap();
        let nb2 = recv_blocking(&cons, 1, T).unwrap();

        assert_eq!(nb3.buffer_id(), 3);
        assert_eq!(nb5.buffer_id(), 5 % 5);
        assert_eq!(nb1.buffer_id(), 1);
        assert_eq!(nb2.buffer_id(), 2);
        assert_eq!(nb3.data().unwrap()[0], 60);
        assert_eq!(nb5.data().unwrap()[0], 70);
        assert_eq!(nb1.data().unwrap()[0], 80);
        assert_eq!(nb2.data().unwrap()[0], 90);

        assert_eq!(b4.data().unwrap()[0], 20); // b4 still holds old value

        drop(b4); drop(nb5);
        push(100); push(100);
        assert_eq!(recv_blocking(&cons, 1, T).unwrap().buffer_id(), 4);
    }

    // -----------------------------------------------------------------------
    // test_register_between_sync_write
    // -----------------------------------------------------------------------
    #[test]
    fn test_register_between_sync_write() {
        let name = uname();
        cleanup(CTX, &name);

        let prod_ctx = Context::new(CTX).unwrap();
        let c1_ctx  = Context::new(CTX).unwrap();
        let prod = prod_ctx.producer(&name, 20, 3, true).unwrap();
        let cons1 = c1_ctx.subscribe(&name).unwrap();

        // send 4 messages, cons1 receives each
        for val in [b"1", b"2", b"3", b"4"] {
            let mut buf = next_blocking(&prod, T).unwrap();
            buf.data_mut().unwrap()[..1].copy_from_slice(val);
            buf.send(1).unwrap();
            let rx = recv_blocking(&cons1, 1, T).unwrap();
            assert_eq!(rx.data().unwrap(), val.as_ref());
            rx.release();
        }

        // Hold a write claim while new consumers subscribe
        let mut b = next_blocking(&prod, T).unwrap();
        b.data_mut().unwrap()[0] = b'5';

        let c2_ctx = Context::new(CTX).unwrap();
        let c3_ctx = Context::new(CTX).unwrap();
        let cons2 = c2_ctx.subscribe(&name).unwrap();
        let cons3 = c3_ctx.subscribe(&name).unwrap();

        // Oldest available at time of subscribe is "3" (buffer_count=3, "4" not yet sent,
        // "3" is the oldest still-available iteration)
        let c2_first = recv_blocking(&cons2, 1, T).unwrap();
        assert_eq!(c2_first.data().unwrap()[0], b'3');
        c2_first.release();

        b.send(1).unwrap();

        let c2_second = recv_blocking(&cons2, 1, T).unwrap();
        assert_eq!(c2_second.data().unwrap()[0], b'4');
        c2_second.release();

        let c2_third = recv_blocking(&cons2, 1, T).unwrap();
        assert_eq!(c2_third.data().unwrap()[0], b'5');
        c2_third.release();

        let c3_first = recv_blocking(&cons3, 1, T).unwrap();
        assert_eq!(c3_first.data().unwrap()[0], b'3');
        c3_first.release();

        let c3_second = recv_blocking(&cons3, 1, T).unwrap();
        assert_eq!(c3_second.data().unwrap()[0], b'4');
        c3_second.release();

        let c3_third = recv_blocking(&cons3, 1, T).unwrap();
        assert_eq!(c3_third.data().unwrap()[0], b'5');
        c3_third.release();

        assert!(cons2.receive(1).is_none());
        assert!(cons3.receive(1).is_none());
    }

    // -----------------------------------------------------------------------
    // test_register_during_sync_write_claim
    // -----------------------------------------------------------------------
    #[test]
    fn test_register_during_sync_write_claim() {
        let name = uname();
        cleanup(CTX, &name);

        let prod_ctx = Context::new(CTX).unwrap();
        let c1_ctx   = Context::new(CTX).unwrap();
        let c2_ctx   = Context::new(CTX).unwrap();
        let prod  = prod_ctx.producer(&name, 20, 2, true).unwrap();
        let cons1 = c1_ctx.subscribe(&name).unwrap();
        let cons2 = c2_ctx.subscribe(&name).unwrap();

        assert_eq!(prod.subscriber_count(), 2);

        // Send buffer 1; unsubscribe cons1 before sending
        {
            let mut b = next_blocking(&prod, T).unwrap();
            assert_eq!(b.buffer_id(), 1);
            b.data_mut().unwrap()[..3].copy_from_slice(b"001");
            drop(cons1);
            assert_eq!(prod.subscriber_count(), 1);
            b.send(3).unwrap();

            let rx = recv_blocking(&cons2, 1, T).unwrap();
            assert_eq!(rx.buffer_id(), 1);
            assert_eq!(rx.data().unwrap(), b"001");
        }

        // Send buffer 2
        {
            let mut b = next_blocking(&prod, T).unwrap();
            assert_eq!(b.buffer_id(), 2 % 2);
            b.data_mut().unwrap()[..3].copy_from_slice(b"002");
            b.send(3).unwrap();
            let rx = recv_blocking(&cons2, 1, T).unwrap();
            assert_eq!(rx.buffer_id(), 2 % 2);
            assert_eq!(rx.data().unwrap(), b"002");
        }

        // Send buffer 3 — writer must not wait for the unsubscribed reader
        {
            let mut b = next_blocking(&prod, T).unwrap();
            assert_eq!(b.buffer_id(), 3 % 2);
            b.data_mut().unwrap()[..3].copy_from_slice(b"003");
            b.send(3).unwrap();
            let rx = recv_blocking(&cons2, 1, T).unwrap();
            assert_eq!(rx.buffer_id(), 3 % 2);
            assert_eq!(rx.data().unwrap(), b"003");
        }
    }

    // -----------------------------------------------------------------------
    // test_unregister_during_sync_write_claim
    // -----------------------------------------------------------------------
    #[test]
    fn test_unregister_during_sync_write_claim() {
        let name = uname();
        cleanup(CTX, &name);

        let prod_ctx = Context::new(CTX).unwrap();
        let c1_ctx   = Context::new(CTX).unwrap();
        let c2_ctx   = Context::new(CTX).unwrap();
        let c3_ctx   = Context::new(CTX).unwrap();
        let prod  = prod_ctx.producer(&name, 20, 5, true).unwrap();
        let cons1 = c1_ctx.subscribe(&name).unwrap();
        let cons2 = c2_ctx.subscribe(&name).unwrap();
        let cons3 = c3_ctx.subscribe(&name).unwrap();

        // All three consumers receive each of "1", "2", "3"
        for val in [b"1".as_ref(), b"2", b"3"] {
            let mut buf = next_blocking(&prod, T).unwrap();
            buf.data_mut().unwrap()[..1].copy_from_slice(val);
            buf.send(1).unwrap();
            for c in [&cons1, &cons2, &cons3] {
                let rx = recv_blocking(c, 1, T).unwrap();
                assert_eq!(rx.data().unwrap(), val);
                rx.release();
            }
        }

        // Drop cons1 mid-stream
        {
            let mut b = next_blocking(&prod, T).unwrap();
            b.data_mut().unwrap()[..1].copy_from_slice(b"4");
            b.send(1).unwrap();
        }
        drop(cons1);
        for c in [&cons2, &cons3] {
            let rx = recv_blocking(c, 1, T).unwrap();
            assert_eq!(rx.data().unwrap(), b"4");
            rx.release();
        }

        // Drop cons2 while holding a write claim
        let mut b = next_blocking(&prod, T).unwrap();
        b.data_mut().unwrap()[..1].copy_from_slice(b"5");
        drop(cons2);
        b.send(1).unwrap();
        {
            let rx = recv_blocking(&cons3, 1, T).unwrap();
            assert_eq!(rx.data().unwrap(), b"5");
            rx.release();
        }

        // Continue with only cons3
        for val in [b"6".as_ref(), b"7", b"8", b"9", b"10", b"11", b"12"] {
            let mut buf = next_blocking(&prod, T).unwrap();
            buf.data_mut().unwrap()[..val.len()].copy_from_slice(val);
            buf.send(val.len()).unwrap();
            let rx = recv_blocking(&cons3, 1, T).unwrap();
            assert_eq!(rx.data().unwrap(), val);
            rx.release();
        }
    }

    // -----------------------------------------------------------------------
    // test_slow_consumer_streaming
    // -----------------------------------------------------------------------
    #[test]
    fn test_slow_consumer_streaming() {
        let name = uname();
        cleanup(CTX, &name);

        let prod_ctx = Context::new(CTX).unwrap();
        let cons_ctx = Context::new(CTX).unwrap();
        let prod = prod_ctx.producer(&name, 20, 3, false).unwrap(); // streaming
        let cons = cons_ctx.subscribe(&name).unwrap();

        let send = |s: &[u8]| {
            let mut buf = next_blocking(&prod, T).unwrap();
            buf.data_mut().unwrap()[..s.len()].copy_from_slice(s);
            buf.send(s.len()).unwrap();
        };

        send(b"1"); send(b"2"); send(b"3");
        // Consumer picks up the oldest available — should be "1"
        let r = recv_blocking(&cons, 1, T).unwrap();
        assert_eq!(r.data().unwrap(), b"1");
        r.release();

        send(b"4"); send(b"5"); send(b"6");
        // After 3 more sends (wrapping the 3-buffer pool), oldest available is "5"
        let r = recv_blocking(&cons, 1, T).unwrap();
        assert_eq!(r.data().unwrap(), b"5");
        r.release();
    }

    // -----------------------------------------------------------------------
    // test_increment_streaming
    // -----------------------------------------------------------------------
    #[test]
    fn test_increment_streaming() {
        let name = uname();
        cleanup(CTX, &name);

        let prod_ctx = Context::new(CTX).unwrap();
        let cons_ctx = Context::new(CTX).unwrap();
        let prod = prod_ctx.producer(&name, 3, 10, false).unwrap();
        let cons = cons_ctx.subscribe(&name).unwrap();

        let send = |s: &[u8]| {
            let mut buf = next_blocking(&prod, T).unwrap();
            buf.data_mut().unwrap()[..s.len()].copy_from_slice(s);
            buf.send(s.len()).unwrap();
        };

        for v in 0u8..10 { send(v.to_string().as_bytes()); }

        let first = recv_blocking(&cons, 1, T).unwrap();
        assert_eq!(first.iteration(), 1);
        first.release();

        for v in 10u8..20 { send(v.to_string().as_bytes()); }

        assert_eq!(cons.last_sent_iteration(), 20);
        assert_eq!(prod.last_sent_iteration(), 20);

        for expected_iter in 12u64..=20 {
            let buf = recv_blocking(&cons, 1, T).unwrap();
            assert_eq!(buf.iteration(), expected_iter);
        }
    }
}
