use crate::{Count, Payload, PayloadBlock, BACKLOG_BUFFER_PAYLOADS, BLOCK_PAYLOADS, UDP_PAYLOAD};
use socket2::{Domain, Socket, Type};
use std::{
    collections::HashMap,
    mem::MaybeUninit,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::{Duration, Instant},
};
use thingbuf::mpsc::blocking::SendRef;
use thiserror::Error;

#[derive(Error, Debug)]
/// Errors that can be produced from captures
pub enum Error {
    #[error("We recieved a payload which wasn't the size we expected {0}")]
    SizeMismatch(usize),
}

pub struct Capture {
    pub sock: Socket,
    pub buffer: [MaybeUninit<u8>; UDP_PAYLOAD],
    pub backlog: HashMap<Count, Payload>,
    pub drops: usize,
    pub processed: usize,
    first_payload: bool,
    oldest_count: u64,
}

impl Capture {
    pub fn new(port: u16) -> anyhow::Result<Self> {
        // Create UDP socket
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, None)?;
        // Bind our listening address
        let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port);
        socket.bind(&address.into())?;
        // Reuse local address without timeout
        socket.reuse_address()?;
        // Set the buffer size to 256 MB (as was done in STARE2)
        let sock_buf_size = 256 * 1024 * 1024;
        socket.set_recv_buffer_size(sock_buf_size)?;
        // Set to nonblocking
        socket.set_nonblocking(true)?;
        Ok(Self {
            sock: socket,
            buffer: [MaybeUninit::uninit(); UDP_PAYLOAD],
            backlog: HashMap::with_capacity(BACKLOG_BUFFER_PAYLOADS),
            drops: 0,
            processed: 0,
            first_payload: false,
            oldest_count: 0,
        })
    }

    pub fn capture(&mut self) -> anyhow::Result<()> {
        loop {
            match self.sock.recv_from(&mut self.buffer) {
                Ok((size, _)) => {
                    if size == UDP_PAYLOAD {
                        // Buffer now contains valid data
                        break;
                    } else {
                        return Err(Error::SizeMismatch(size).into());
                    }
                }
                Err(e) => {
                    if let Some(os) = e.raw_os_error() {
                        // EAGAIN
                        if os != 11 {
                            return Err(e.into());
                        }
                    }
                }
            }
            std::hint::spin_loop();
        }
        Ok(())
    }

    pub fn next_block(
        &mut self,
        mut block_slot: SendRef<'_, PayloadBlock>,
    ) -> anyhow::Result<(Duration, Duration)> {
        let block_size = block_slot.0.len();
        // Sneaky bit manipulation (all bits to 1 to set that the index corresponding with *that bit* needs to be filled)
        let mut to_fill = BLOCK_PAYLOADS - 1;
        // Create a timer for average block processing
        let mut packet_time = Duration::default();
        // Iterate through every payload in this block
        for _ in 0..block_size {
            // ----- CAPTURE
            // Capture the next payload
            if let Err(e) = self.capture() {
                match e.downcast() {
                    Ok(e) => match e {
                        // Just drop and continue of corrupt payloads
                        Error::SizeMismatch(_) => continue,
                    },
                    Err(e) => return Err(e),
                }
            }
            // Start the packet processing timer
            let now = Instant::now();

            // Data is now valid
            let buffer = unsafe { std::mem::transmute(self.buffer) };

            // Decode its count
            let count = count(&buffer);
            if self.first_payload {
                self.oldest_count = count;
                self.first_payload = false;
            }
            // ----- SORT
            // Find its position in this block
            if count < self.oldest_count {
                // Drop this payload, it happened in the past
                self.drops += 1;
            } else if count >= self.oldest_count + block_size as u64 {
                // Packet is destined for the future, insert into reorder buf
                self.backlog.insert(count, buffer);
            } else {
                let idx = (count - self.oldest_count) as usize;
                // Remove this idx from the `to_fill` entry
                to_fill &= !(1 << idx);
                // Packet is for this block! Insert into it's position
                // Safety: the index is correct by construction as count-oldest_count will always be inbounds
                block_slot.0[idx].write(buffer);
                self.processed += 1;
            }
            // Stop the timer and add to the block time
            packet_time += now.elapsed();
        }
        // Now we'll fill in gaps with past data, if we have it
        // Otherwise replace with zeros and increment the drop count
        let block_process = Instant::now();
        for (idx, buf) in block_slot.0.iter_mut().enumerate() {
            // Check if this bit needs to be filled
            if (to_fill >> idx) & 1 == 1 {
                // Then either fill with data from the past, or set it as default
                let count = idx as u64 + self.oldest_count;
                if let Some(pl) = self.backlog.remove(&count) {
                    buf.write(pl);
                    self.processed += 1;
                } else {
                    let mut pl = [0u8; UDP_PAYLOAD];
                    (pl[0..8]).clone_from_slice(&count.to_be_bytes());
                    buf.write(pl);
                    self.drops += 1;
                }
            }
        }
        // Move the oldest count forward by the block size
        self.oldest_count += block_size as u64;
        let block_process_time = block_process.elapsed();
        // Return timing info
        Ok((packet_time, block_process_time))
    }
}

fn count(pl: &Payload) -> Count {
    u64::from_be_bytes(pl[0..8].try_into().unwrap())
}
