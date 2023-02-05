mod capture;

use crate::capture::Capture;
use anyhow::bail;
use core_affinity::CoreId;
use std::mem::MaybeUninit;
use thingbuf::{mpsc::with_recycle, Recycle};

const UDP_PAYLOAD: usize = 8200;
const WARMUP_PACKETS: usize = 1_000_000;
const BACKLOG_BUFFER_PAYLOADS: usize = 4096;
const BLOCK_PAYLOAD_POW: u32 = 8;
const BLOCK_PAYLOADS: usize = 2usize.pow(BLOCK_PAYLOAD_POW);
const BLOCKS_TO_SORT: usize = 512;

type Count = u64;

pub type Payload = [u8; UDP_PAYLOAD];

#[derive(Clone)]
pub struct PayloadBlock([MaybeUninit<Payload>; BLOCK_PAYLOADS]);

pub struct PayloadRecycle;

impl PayloadRecycle {
    pub const fn new() -> Self {
        Self
    }
}

impl Recycle<PayloadBlock> for PayloadRecycle {
    fn new_element(&self) -> PayloadBlock {
        PayloadBlock([MaybeUninit::uninit(); BLOCK_PAYLOADS])
    }

    fn recycle(&self, _: &mut PayloadBlock) {
        // Do nothing, we have to be careful about uninit
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    // Setup the monitoring
    console_subscriber::init();

    // Bind this thread to a core that shares a NUMA node with the NIC
    if !core_affinity::set_for_current(CoreId { id: 8 }) {
        bail!("Couldn't set core affinity");
    }

    // Create the socket
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
        let slot = s.send_ref().await.unwrap();
        let (p, b) = cap.next_block(slot).await?;
        // Print timing info
        println!(
            "Processing - {} us per packet\tBlock - {} us",
            p.as_micros() as f32 / BLOCK_PAYLOADS as f32,
            b.as_micros()
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
