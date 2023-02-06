mod capture;

use crate::capture::Capture;
use anyhow::bail;
use core_affinity::CoreId;
use std::time::Instant;
use thingbuf::{mpsc::blocking::with_recycle, Recycle};

const UDP_PAYLOAD: usize = 8200;
const WARMUP_PACKETS: usize = 1024; // If the receive buffer is only 256 MB, this should be plenty
const BACKLOG_BUFFER_PAYLOADS: usize = 1024; // It should never exceed this
const BLOCK_PAYLOAD_POW: u32 = 15;
const BLOCK_PAYLOADS: usize = 2usize.pow(BLOCK_PAYLOAD_POW);
const BLOCKS_TO_SORT: usize = 512;
const RING_BLOCKS: usize = 4;

type Count = u64;

pub type Payload = [u8; UDP_PAYLOAD];

pub type PayloadBlock = Box<[Payload; BLOCK_PAYLOADS]>;

pub struct PayloadRecycle;

impl PayloadRecycle {
    pub const fn new() -> Self {
        Self
    }
}

impl Recycle<PayloadBlock> for PayloadRecycle {
    fn new_element(&self) -> PayloadBlock {
        Box::new([[0u8; UDP_PAYLOAD]; BLOCK_PAYLOADS])
    }

    fn recycle(&self, _: &mut PayloadBlock) {
        // Do nothing, we will write to every position anyway
    }
}

fn main() -> anyhow::Result<()> {
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
        s.send_ref()?;
        r.recv_ref();
    }

    // Spawn a thread to "sink" the payloads
    std::thread::spawn(move || {
        core_affinity::set_for_current(CoreId { id: 9 });
        // Create a "static" buffer for this thread so we don't alloc
        let mut current_block = Box::new([[0u8; UDP_PAYLOAD]; BLOCK_PAYLOADS]);
        while let Some(block) = r.recv_ref() {
            // Copy into thread memory and drop
            current_block.clone_from(&block);
            let now = Instant::now();
            // Do some work, maybe add all the numbers together. This should take on order 35ms (overflowing, but we don't care yet)
            let sum = current_block
                .iter()
                .fold(0u8, |x, y| x + y.iter().sum::<u8>());
            println!("Sum - {sum}, Duration - {} ms", now.elapsed().as_millis())
        }
    });

    // "Warm up" by capturing a ton of packets
    for _ in 0..WARMUP_PACKETS {
        cap.capture()?;
    }

    // Sort N blocks, printing dropped packets
    for _ in 0..BLOCKS_TO_SORT {
        // First block to grab a reference to the next slot in the queue
        let slot = s.send_ref().unwrap();

        // Fill a block
        let (p, b) = cap.capture_next_block(slot)?;

        // At this point, we'd send the "sorted" block to the next stage by dropping slot
        // Print timing info
        println!(
            "Processing - {} us per packet\tBlock - {} us - Backlog {}",
            p.as_micros() as f32 / BLOCK_PAYLOADS as f32,
            b.as_micros(),
            cap.backlog.len(),
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
