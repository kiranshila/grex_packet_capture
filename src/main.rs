mod capture;

use crate::capture::Capture;
use anyhow::bail;
use core_affinity::CoreId;
use std::{
    collections::HashMap,
    mem::MaybeUninit,
    time::{Duration, Instant},
};
use thingbuf::{mpsc::with_recycle, Recycle};
use tokio::net::UdpSocket;

const UDP_PAYLOAD: usize = 8200;
const WARMUP_PACKETS: usize = 1_000_000;
const BACKLOG_BUFFER_PAYLOADS: usize = 4096;
const BLOCK_PAYLOAD_POW: u32 = 15;
const BLOCK_PAYLOADS: usize = 2usize.pow(BLOCK_PAYLOAD_POW);
const BLOCKS_TO_SORT: usize = 512;

async fn capture(sock: &UdpSocket, buf: &mut [u8]) -> anyhow::Result<()> {
    let n = sock.recv(buf).await?;
    if n != buf.len() {
        bail!("Wrong size");
    }
    Ok(())
}

type Count = u64;

pub type Payload = [u8; UDP_PAYLOAD];

fn count(pl: &Payload) -> Count {
    u64::from_be_bytes(pl[0..8].try_into().unwrap())
}

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
    let cap = Capture::new(60000)?;

    // Create some state
    let mut buffer = [0u8; UDP_PAYLOAD];
    let mut backlog = HashMap::with_capacity(BACKLOG_BUFFER_PAYLOADS);
    // Sneaky bit manipulation (all bits to 1 to set that the index corresponding with *that bit* needs to be filled)
    let mut to_fill = BLOCK_PAYLOADS - 1;

    // Create the channel to bench the copies
    let (s, r) = with_recycle(4, PayloadRecycle::new());

    // Spawn a task to "sink" the payloads
    tokio::spawn(async move { while r.recv_ref().await.is_some() {} });

    // "Warm up" by capturing a ton of packets
    for _ in 0..WARMUP_PACKETS {
        capture(&cap.sock, &mut buffer).await?;
    }

    let mut first_payload = true;
    let mut oldest_count = 0;
    let mut drops = 0;
    let mut processed = 0;

    // Sort N blocks, printing dropped packets
    for _ in 0..BLOCKS_TO_SORT {
        // First block to grab a reference to the next slot in the queue
        let mut slot = s.send_ref().await.unwrap();

        // Create a timer for average block processing
        let mut time = Duration::default();

        for _ in 0..slot.0.len() {
            // ----- CAPTURE

            // Capture an arbitrary payload
            capture(&cap.sock, &mut buffer).await?;

            // Time starts now to benchmark processing perf
            let now = Instant::now();

            // Decode its count
            let count = count(&buffer);
            if first_payload {
                oldest_count = count;
                first_payload = false;
            }

            // ----- SORT

            // Find its position in this block
            if count < oldest_count {
                // Drop this payload, it happened in the past
                drops += 1;
            } else if count >= oldest_count + slot.0.len() as u64 {
                // Packet is destined for the future, insert into reorder buf
                backlog.insert(count, buffer);
            } else {
                let idx = (count - oldest_count) as usize;
                // Remove this idx from the `to_fill` entry
                to_fill &= !(1 << idx);
                // Packet is for this block! Insert into it's position
                // Safety: the index is correct by construction as count-oldest_count will always be inbounds
                slot.0[idx].write(buffer);
                processed += 1;
            }

            // Stop the timer and add to the block time
            time += now.elapsed();
        }
        // Now we'll fill in gaps with past data, if we have it
        // Otherwise replace with zeros and increment the drop count
        let block_process = Instant::now();

        for (idx, buf) in slot.0.iter_mut().enumerate() {
            // Check if this bit needs to be filled
            if (to_fill >> idx) & 1 == 1 {
                // Then either fill with data from the past, or set it as default
                let count = idx as u64 + oldest_count;
                if let Some(pl) = backlog.remove(&count) {
                    buf.write(pl);
                    processed += 1;
                } else {
                    let mut pl = [0u8; UDP_PAYLOAD];
                    (pl[0..8]).clone_from_slice(&count.to_be_bytes());
                    buf.write(pl);
                    drops += 1;
                }
            }
        }

        // Then reset to_fill
        to_fill = BLOCK_PAYLOADS - 1;
        // Move the oldest count forward by the block size
        oldest_count += slot.0.len() as u64;
        let block_process_time = block_process.elapsed();
        // At this point, we'd send the "sorted" block to the next stage by dropping slot
        // Print timing info
        println!(
            "Processing - {} us per packet\tBlock - {} us",
            time.as_micros() as f32 / BLOCK_PAYLOADS as f32,
            block_process_time.as_micros()
        );
    }

    println!("Dropped {drops} packets while processing {processed} packets.");
    println!(
        "That's a drop rate of {}%",
        100.0 * drops as f32 / (drops + processed) as f32
    );
    Ok(())
}
