mod capture;

use crate::capture::Capture;
use anyhow::bail;
use core_affinity::CoreId;
use std::time::{Duration, Instant};
use thingbuf::{mpsc::blocking::with_recycle, Recycle};

const UDP_PAYLOAD: usize = 8200;
const WARMUP_PACKETS: usize = 1024; // If the receive buffer is only 256 MB, this should be plenty
const BACKLOG_BUFFER_PAYLOADS: usize = 4096;
const BLOCK_PAYLOAD_POW: u32 = 15;
const BLOCK_PAYLOADS: usize = 2usize.pow(BLOCK_PAYLOAD_POW);
const BLOCKS_TO_SORT: usize = 512;
const RING_BLOCKS: usize = 4;

type Count = u64;

pub type Payload = [u8; UDP_PAYLOAD];

fn count(pl: &Payload) -> Count {
    u64::from_be_bytes(pl[0..8].try_into().unwrap())
}

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

    // Sneaky bit manipulation (all bits to 1 to set that the index corresponding with *that bit* needs to be filled)
    let mut to_fill = BLOCK_PAYLOADS - 1;

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

    let mut first_payload = true;
    let mut oldest_count = 0;
    let mut drops = 0;
    let mut processed = 0;

    // Sort N blocks, printing dropped packets
    for _ in 0..BLOCKS_TO_SORT {
        // First block to grab a reference to the next slot in the queue
        let mut slot = s.send_ref().unwrap();

        // Create a timer for average block processing
        let mut time = Duration::default();

        for _ in 0..slot.len() {
            // ----- CAPTURE

            // Capture an arbitrary payload
            cap.capture()?;

            // Time starts now to benchmark processing perf
            let now = Instant::now();

            // Decode its count
            let count = count(&cap.buffer);
            if first_payload {
                oldest_count = count;
                first_payload = false;
            }

            // ----- SORT

            // Find its position in this block
            if count < oldest_count {
                // Drop this payload, it happened in the past
                drops += 1;
            } else if count >= oldest_count + slot.len() as u64 {
                // Packet is destined for the future, insert into reorder buf
                cap.backlog.insert(count, cap.buffer);
            } else {
                let idx = (count - oldest_count) as usize;
                // Remove this idx from the `to_fill` entry
                to_fill &= !(1 << idx);
                // Packet is for this block! Insert into it's position
                slot[idx] = cap.buffer;
                processed += 1;
            }

            // Stop the timer and add to the block time
            time += now.elapsed();
        }
        // Now we'll fill in gaps with past data, if we have it
        // Otherwise replace with zeros and increment the drop count
        let block_process = Instant::now();

        for (idx, buf) in slot.iter_mut().enumerate() {
            // Check if this bit needs to be filled
            if (to_fill >> idx) & 1 == 1 {
                // Then either fill with data from the past, or set it as default
                let count = idx as u64 + oldest_count;
                if let Some(pl) = cap.backlog.remove(&count) {
                    buf.clone_from_slice(&pl);
                    processed += 1;
                } else {
                    let mut pl = [0u8; UDP_PAYLOAD];
                    (pl[0..8]).clone_from_slice(&count.to_be_bytes());
                    buf.clone_from_slice(&pl);
                    drops += 1;
                }
            }
        }

        // Then reset to_fill
        to_fill = BLOCK_PAYLOADS - 1;
        // Move the oldest count forward by the block size
        oldest_count += slot.len() as u64;
        let block_process_time = block_process.elapsed();
        // At this point, we'd send the "sorted" block to the next stage by dropping slot
        // Print timing info
        println!(
            "Processing - {} us per packet\tBlock - {} us - Backlog {}",
            time.as_micros() as f32 / BLOCK_PAYLOADS as f32,
            block_process_time.as_micros(),
            cap.backlog.len(),
        );
    }

    println!("Dropped {drops} packets while processing {processed} packets.");
    println!(
        "That's a drop rate of {}%",
        100.0 * drops as f32 / (drops + processed) as f32
    );
    Ok(())
}
