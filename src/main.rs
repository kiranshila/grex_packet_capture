use anyhow::bail;
use core_affinity::CoreId;
use libc::EAGAIN;
use socket2::{Domain, Socket, Type};
use std::{
    collections::{HashMap, VecDeque},
    hint::{self, spin_loop},
    mem::MaybeUninit,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use thingbuf::{Recycle, ThingBuf};

const UDP_PAYLOAD: usize = 8200;
const WARMUP_PACKETS: usize = 1_000_000;
const BACKLOG_BUFFER_PAYLOADS: usize = 1024;
const BLOCK_PAYLOADS: usize = 32_768;
const BLOCKS_TO_SORT: usize = 512;

fn clear_buffered_packets(
    sock: &Socket,
    packet_buffer: &mut [MaybeUninit<u8>],
) -> anyhow::Result<usize> {
    let mut bytes_cleared = 0;
    loop {
        match sock.recv_from(packet_buffer) {
            Ok((size, _)) => bytes_cleared += size,
            Err(e) => {
                if let Some(os) = e.raw_os_error() {
                    if os != EAGAIN {
                        return Err(e.into());
                    }
                }
                break;
            }
        }
    }
    Ok(bytes_cleared)
}

fn capture(sock: &Socket, packet_buffer: &mut [MaybeUninit<u8>]) -> anyhow::Result<()> {
    loop {
        match sock.recv_from(packet_buffer) {
            Ok((size, _)) => {
                if size == UDP_PAYLOAD {
                    // Buffer now contains valid data
                    break;
                }
            }
            Err(e) => {
                if let Some(os) = e.raw_os_error() {
                    if os != EAGAIN {
                        return Err(e.into());
                    }
                }
            }
        }
        hint::spin_loop();
    }
    Ok(())
}

type Count = u64;

#[derive(Debug, Clone, Copy)]
struct Payload([u8; UDP_PAYLOAD]);

impl Payload {
    fn count(&self) -> Count {
        u64::from_be_bytes(self.0[0..8].try_into().unwrap())
    }
}

impl Default for Payload {
    fn default() -> Self {
        Self([0u8; UDP_PAYLOAD])
    }
}

struct ReorderBuffer {
    backlog_idxs: HashMap<Count, usize>,
    buffer: Vec<Payload>,
    free_idxs: VecDeque<usize>,
}

impl ReorderBuffer {
    fn new(size: usize) -> Self {
        Self {
            backlog_idxs: HashMap::new(),
            buffer: vec![Payload::default(); size],
            free_idxs: (0..size).collect(),
        }
    }
    fn insert(&mut self, payload: Payload) -> anyhow::Result<()> {
        // Grab the next free index
        let idx = match self.free_idxs.pop_front() {
            Some(idx) => idx,
            None => bail!("Reorder buffer filled up, this shouldn't happen"),
        };

        // Associate its count
        let count = payload.count();
        self.backlog_idxs.insert(count, idx);
        // Memcpy into buffer
        // Safety: These indicies are correct by construction
        *unsafe { self.buffer.get_unchecked_mut(idx) } = payload;
        Ok(())
    }
    fn remove(&mut self, count: &Count) -> Option<Payload> {
        // Try to find the associated entry
        let idx = self.backlog_idxs.remove(count)?;
        // Recycle the index
        self.free_idxs.push_back(idx);
        // Return the data
        // Safety: These indicies are correct by construction
        Some(*unsafe { self.buffer.get_unchecked(idx) })
    }
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

fn main() -> anyhow::Result<()> {
    // Bind this thread to a core that shares a NUMA node with the NIC
    if !core_affinity::set_for_current(CoreId { id: 8 }) {
        bail!("Couldn't set core affinity");
    }
    // Create UDP socket
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, None)?;
    // Bind our listening address
    let address: SocketAddr = "0.0.0.0:60000".parse().unwrap();
    socket.bind(&address.into())?;
    // Reuse local address without timeout
    socket.reuse_address()?;
    // Set the buffer size to 256 MB (as was done in STARE2)
    let sock_buf_size = 256 * 1024 * 1024;
    socket.set_recv_buffer_size(sock_buf_size)?;
    // Set to nonblocking
    socket.set_nonblocking(true)?;

    // Create some state
    let mut buffer = [MaybeUninit::zeroed(); UDP_PAYLOAD];
    let mut rb = ReorderBuffer::new(BACKLOG_BUFFER_PAYLOADS);
    let mut to_fill = vec![true; BLOCK_PAYLOADS];

    // Create the channel to bench the copies
    let source_buf = Arc::new(ThingBuf::with_recycle(4, PayloadRecycle::new()));

    let sink_buf = source_buf.clone();

    // Spawn a thread to "sink" the payloads
    let handle = std::thread::spawn(move || {
        // Pin to the next core on the numa node
        if !core_affinity::set_for_current(CoreId { id: 9 }) {
            panic!("Couldn't set core affinity");
        }
        let mut count = 0;
        loop {
            if sink_buf.pop().is_some() {
                count += 1;
                if count == BLOCKS_TO_SORT {
                    break;
                }
            } else {
                spin_loop();
            }
        }
    });

    // Clear buffered packets
    clear_buffered_packets(&socket, &mut buffer)?;

    // "Warm up" by capturing a ton of packets
    for _ in 0..WARMUP_PACKETS {
        capture(&socket, &mut buffer)?;
    }

    let mut first_payload = true;
    let mut oldest_count = 0;
    let mut drops = 0;
    let mut processed = 0;

    // Sort N blocks, printing dropped packets
    for _ in 0..BLOCKS_TO_SORT {
        // First block to grab a reference to the next slot in the queue
        let mut slot = loop {
            match source_buf.push_ref() {
                Ok(slot) => {
                    break slot;
                }
                Err(_) => spin_loop(),
            }
        };

        // Create a timer for average block processing
        let mut time = Duration::default();

        for _ in 0..slot.0.len() {
            // ----- CAPTURE

            // Capture an arbitrary payload
            capture(&socket, &mut buffer)?;

            // Time starts now to benchmark processing perf
            let now = Instant::now();

            // Safety: buffer is now init, otherwise capture would have failed
            let pl = Payload(unsafe { std::mem::transmute(buffer) });
            // Decode its count
            let count = pl.count();
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
                // If the buffer fills up, panic. This shouldn't happen.
                rb.insert(pl).unwrap();
            } else {
                let idx = (count - oldest_count) as usize;
                // Remove this idx from the `to_fill` entry
                to_fill[idx] = false;
                // Packet is for this block! Insert into it's position
                slot.0[idx].write(pl);
                processed += 1;
            }

            // Stop the timer and add to the block time
            time += now.elapsed();
        }
        // Now we'll fill in gaps with past data, if we have it
        // Otherwise replace with zeros and increment the drop count
        for (idx, _) in to_fill.into_iter().enumerate().filter(|(_, fill)| *fill) {
            let count = idx as u64 + oldest_count;
            if let Some(pl) = rb.remove(&count) {
                slot.0[idx].write(pl);
                processed += 1;
            } else {
                slot.0[idx].write(Payload::default());
                drops += 1;
            }
        }
        // Then reset to_fill
        to_fill = vec![true; BLOCK_PAYLOADS];
        // Move the oldest count forward by the block size
        oldest_count += slot.0.len() as u64;
        // At this point, we'd send the "sorted" block to the next stage by dropping slot
        // Print timing info
        println!(
            "Processing {BLOCK_PAYLOADS} packets took {} seconds, or {} per packet",
            time.as_secs(),
            time.as_secs_f32() / BLOCK_PAYLOADS as f32
        );
    }

    // Join the sink
    handle.join().unwrap();

    println!("Dropped {drops} packets while processing {processed} packets.");
    println!(
        "That's a drop rate of {}%",
        100.0 * drops as f32 / (drops + processed) as f32
    );
    Ok(())
}
