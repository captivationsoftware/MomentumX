/// Cross-language validation: Rust producer.
///
/// Sends MX_MSG_COUNT messages to a MomentumX stream, waits for a
/// subscriber before sending, then calls end() when done.
///
/// Environment variables:
///   MX_CONTEXT      — context path (default: /dev/shm)
///   MX_STREAM_NAME  — stream name  (default: crosslang)
///   MX_MSG_COUNT    — message count (default: 10)
///   MX_SYNC             — sync mode 1/0 (default: 1)
///                         1 = block each send until subscriber acknowledges (guaranteed delivery)
///                         0 = streaming, non-blocking (consumer may miss messages)
///   MX_MIN_SUBSCRIBERS  — minimum subscribers before starting to send (default: 1)
use momentumx::{Context, LogLevel};
use std::time::{Duration, Instant};
use std::{env, thread};

const TIMEOUT: Duration = Duration::from_secs(30);

fn main() {
    let ctx_path = env::var("MX_CONTEXT").unwrap_or_else(|_| "/dev/shm".to_string());
    let stream_name = env::var("MX_STREAM_NAME").unwrap_or_else(|_| "crosslang".to_string());
    let msg_count: usize = env::var("MX_MSG_COUNT")
        .unwrap_or_else(|_| "10".to_string())
        .parse()
        .unwrap_or(10);
    let sync = env::var("MX_SYNC").unwrap_or_else(|_| "1".to_string()) != "0";
    let min_subscribers: usize = env::var("MX_MIN_SUBSCRIBERS")
        .unwrap_or_else(|_| "1".to_string())
        .parse()
        .unwrap_or(1);

    println!("[rust-producer] ctx={ctx_path}  stream={stream_name}  count={msg_count}  sync={sync}  min_subs={min_subscribers}");

    let ctx = Context::new(&ctx_path).expect("create context");
    ctx.set_log_level(LogLevel::Warning);

    let stream = ctx
        .producer(&stream_name, 256, 4, sync)
        .expect("create producer stream");

    println!("[rust-producer] waiting for {min_subscribers} subscriber(s)...");
    let deadline = Instant::now() + TIMEOUT;
    while stream.subscriber_count() < min_subscribers {
        if Instant::now() > deadline {
            eprintln!("[rust-producer] timed out waiting for subscribers (have {}, need {})", stream.subscriber_count(), min_subscribers);
            std::process::exit(1);
        }
        thread::sleep(Duration::from_millis(50));
    }
    println!("[rust-producer] {} subscriber(s) connected, sending {msg_count} messages", stream.subscriber_count());

    for i in 0..msg_count {
        let msg = format!("hello from rust {i}");
        let deadline = Instant::now() + TIMEOUT;
        loop {
            if let Some(mut buf) = stream.next() {
                let len = {
                    let data = buf.data_mut().expect("data_mut");
                    let bytes = msg.as_bytes();
                    data[..bytes.len()].copy_from_slice(bytes);
                    bytes.len()
                };
                buf.send(len).expect("send");
                println!("[rust-producer] sent [{i}]: {msg}");
                break;
            }
            if Instant::now() > deadline {
                eprintln!("[rust-producer] timed out waiting for buffer");
                std::process::exit(1);
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    stream.end();
    println!("[rust-producer] done — stream ended");
}
