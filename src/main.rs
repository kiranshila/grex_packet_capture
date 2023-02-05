mod capture;

use crate::capture::{Capture, PayloadRecycle};
use anyhow::bail;
use core_affinity::CoreId;
use thingbuf::mpsc::with_recycle;

const WARMUP_PACKETS: usize = 1_000_000;
const BLOCKS_TO_SORT: usize = 512;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    // Setup the monitoring
    console_subscriber::init();

    // Bind this thread to a core that shares a NUMA node with the NIC
    if !core_affinity::set_for_current(CoreId { id: 8 }) {
        bail!("Couldn't set core affinity");
    }

    // Create our capture
    let mut cap = Capture::new(60000)?;

    // Create the channel to bench the copies
    let (s, r) = with_recycle(4, PayloadRecycle::new());

    // Spawn a task to "sink" the payloads
    tokio::spawn(async move { while r.recv_ref().await.is_some() {} });

    // "Warm up" by capturing a ton of packets
    for _ in 0..WARMUP_PACKETS {
        cap.capture().await?;
    }

    // Sort N blocks, printing dropped packets
    for _ in 0..BLOCKS_TO_SORT {
        // First block to grab a reference to the next slot in the queue
        let mut slot = s.send_ref().await.unwrap();
        // Then perform the sorting capture
        let (packet, block) = cap.capture_sorted_block(&mut slot).await?;
        // Lexical scope means slot is dropped and therefore "sent"
        // Print timing info
        println!(
            "Processing - {} per packet\tBlock - {}",
            packet.as_micros(),
            block.as_micros()
        );
    }

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
