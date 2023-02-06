mod capture;

use crate::capture::{boxed_payload, Capture, PayloadRecycle};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use thingbuf::mpsc::with_recycle;
use tokio::task;

const RING_BLOCKS: usize = 1024;
const PAYLOADS_TO_SORT: usize = 32768 * 512;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    // Create the channel to bench the copies
    let (s, r) = with_recycle(RING_BLOCKS, PayloadRecycle::new());
    // Create the socket
    let mut cap = Capture::new(60000)?;
    // Preallocate the buffer with non-uninit values
    for _ in 0..RING_BLOCKS {
        s.send_ref().await?;
        r.recv_ref().await;
    }
    // Create the atomic bool for shutdown
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();
    // Spawn a thread to "sink" the payloads
    let cap_sum = tokio::spawn(async move {
        // Create a "static" buffer for this thread so we don't alloc
        let mut current_payload = boxed_payload();
        let mut global_sum = 0;
        let mut payloads = 0;
        while let Some(payload) = r.recv_ref().await {
            payloads += 1;
            // Copy into thread memory and drop
            current_payload.clone_from(&payload);
            // Do some work, maybe add all the numbers together. This should take on order 35ms (overflowing, but we don't care yet)
            global_sum += current_payload.iter().sum::<u8>();
            if payloads == PAYLOADS_TO_SORT {
                shutdown_clone.store(true, Ordering::Release);
            }
        }
        global_sum
    });

    // Sort N blocks, printing dropped packets
    let total_time = Duration::default();
    task::unconstrained(cap.start(s, shutdown.as_ref())).await?;

    println!("Sum of all the bytes- {}", cap_sum.await.unwrap());
    println!(
        "Average packet processing time is: {}",
        total_time.as_micros() as f32 / PAYLOADS_TO_SORT as f32
    );
    println!(
        "Dropped {} packets while processing {} packets.",
        cap.drops, cap.processed
    );
    println!(
        "That's a drop rate of {}%",
        100.0 * cap.drops as f32 / (cap.drops + cap.processed) as f32
    );
    Ok(())
}
