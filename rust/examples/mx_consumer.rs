/// Cross-language validation: Rust consumer.
///
/// Subscribes to a MomentumX stream (retrying until available), receives
/// all messages until the stream ends, then validates the count.
/// Exits 0 on success, 1 on failure.
///
/// Environment variables:
///   MX_CONTEXT      — context path (default: /dev/shm)
///   MX_STREAM_NAME  — stream name  (default: crosslang)
///   MX_MSG_COUNT    — expected message count (default: 10)
///   MX_STRICT_COUNT — 1/0 (default: 1)
///                     0 = non-sync mode; partial delivery is expected, just log the count
use momentumx::{Context, LogLevel};
use std::time::{Duration, Instant};
use std::{env, thread};

const TIMEOUT: Duration = Duration::from_secs(30);

fn main() {
    let ctx_path = env::var("MX_CONTEXT").unwrap_or_else(|_| "/dev/shm".to_string());
    let stream_name = env::var("MX_STREAM_NAME").unwrap_or_else(|_| "crosslang".to_string());
    let expected: usize = env::var("MX_MSG_COUNT")
        .unwrap_or_else(|_| "10".to_string())
        .parse()
        .unwrap_or(10);
    let strict_count = env::var("MX_STRICT_COUNT").unwrap_or_else(|_| "1".to_string()) != "0";

    println!("[rust-consumer] ctx={ctx_path}  stream={stream_name}  expected={expected}  strict={strict_count}");

    let ctx = Context::new(&ctx_path).expect("create context");
    ctx.set_log_level(LogLevel::Warning);

    let deadline = Instant::now() + TIMEOUT;
    let stream = loop {
        if let Some(s) = ctx.subscribe(&stream_name) {
            break s;
        }
        if Instant::now() > deadline {
            eprintln!("[rust-consumer] timed out waiting for stream to appear");
            std::process::exit(1);
        }
        thread::sleep(Duration::from_millis(100));
    };
    println!("[rust-consumer] subscribed — reading messages");

    let mut received = 0usize;
    // Exit as soon as expected count is reached OR the stream ends — whichever
    // comes first.  This lets the consumer self-terminate without requiring the
    // producer to call end(), enabling clean Rust Drop (→ unsubscribe) as soon
    // as the consumer's work is done.
    while received < expected && stream.has_next() {
        if let Some(buf) = stream.receive(1) {
            let data = buf.data().expect("data");
            let msg = std::str::from_utf8(data).unwrap_or("<invalid utf-8>");
            println!("[rust-consumer] got [{received}]: {msg}");
            received += 1;
        } else {
            thread::sleep(Duration::from_millis(10));
        }
    }

    println!("[rust-consumer] done — received {received}/{expected}");
    if received != expected {
        if strict_count {
            eprintln!("[rust-consumer] FAIL: expected {expected} messages, got {received}");
            std::process::exit(1);
        } else {
            println!("[rust-consumer] OK (non-sync): received {received} of {expected} messages");
            std::process::exit(0);
        }
    }
    println!("[rust-consumer] PASS");
}
