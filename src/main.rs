mod capture;

use crate::capture::Capture;
use anyhow::bail;
use core_affinity::CoreId;
use std::time::{Duration, Instant};
use thingbuf::{mpsc::with_recycle, Recycle};

const UDP_PAYLOAD: usize = 8200;
const WARMUP_PACKETS: usize = 1024; // If the receive buffer is only 256 MB, this should be plenty
const BACKLOG_BUFFER_PAYLOADS: usize = 1024; // It should never exceed this
const PAYLOADS_TO_SORT: usize = 32768 * 512;
const RING_BLOCKS: usize = 1024;

type Count = u64;

pub type Payload = [u8; UDP_PAYLOAD];

pub struct PayloadRecycle;

impl PayloadRecycle {
    pub const fn new() -> Self {
        Self
    }
}

impl Recycle<Box<Payload>> for PayloadRecycle {
    fn new_element(&self) -> Box<Payload> {
        Box::new([0u8; UDP_PAYLOAD])
    }

    fn recycle(&self, _: &mut Box<Payload>) {
        // Do nothing, we will write to every position anyway
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    // Bind this thread to a core that shares a NUMA node with the NIC
    if !core_affinity::set_for_current(CoreId { id: 8 }) {
        bail!("Couldn't set core affinity");
    }
    // Create the socket
    let mut cap = Capture::new(60000)?;
    // Create the channel to bench the copies
    let (s, r) = with_recycle(RING_BLOCKS, PayloadRecycle::new());
    // Preallocate the buffer with non-uninit values
    for _ in 0..RING_BLOCKS {
        s.send_ref().await?;
        r.recv_ref().await;
    }
    // Spawn a thread to "sink" the payloads
    let cap_sum = tokio::spawn(async move {
        // Create a "static" buffer for this thread so we don't alloc
        let mut current_payload = Box::new([0u8; UDP_PAYLOAD]);
        let mut global_sum = 0;
        while let Some(payload) = r.recv_ref().await {
            // Copy into thread memory and drop
            current_payload.clone_from(&payload);
            // Do some work, maybe add all the numbers together. This should take on order 35ms (overflowing, but we don't care yet)
            global_sum += current_payload.iter().sum::<u8>();
        }
        global_sum
    });

    // "Warm up" by capturing a ton of packets
    let mut warmup_buf = Box::new([0u8; UDP_PAYLOAD]);
    for _ in 0..WARMUP_PACKETS {
        cap.capture(&mut warmup_buf).await?;
    }

    // Sort N blocks, printing dropped packets
    let mut total_time = Duration::default();
    for _ in 0..PAYLOADS_TO_SORT {
        // First block to grab a reference to the next payload slot in the queue
        let slot = s.send_ref().await.unwrap();
        // Fill a slot
        total_time += cap.capture_sort(slot).await?;
    }
    drop(s);
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
