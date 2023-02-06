use socket2::{Domain, Socket, Type};
use std::net::UdpSocket;
use std::time::{Duration, Instant};
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};
use thingbuf::mpsc::blocking::SendRef;
use thiserror::Error;

use crate::{Count, Payload, PayloadBlock, BACKLOG_BUFFER_PAYLOADS, UDP_PAYLOAD};

#[derive(Error, Debug)]
/// Errors that can be produced from captures
pub enum Error {
    #[error("We recieved a payload which wasn't the size we expected {0}")]
    SizeMismatch(usize),
    #[error("Failed to set the recv buffer size. We tried to set {expected}, but found {found}. Check sysctl net.core.rmem_max")]
    SetRecvBufferFailed { expected: usize, found: usize },
}

pub struct Capture {
    pub sock: UdpSocket,
    pub buffer: Payload,
    pub backlog: HashMap<Count, Payload>,
    pub drops: usize,
    pub processed: usize,
    first_payload: bool,
    oldest_count: Count,
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
        // Set the buffer size to 256MB (it will read as double, for some reason)
        let sock_buf_size = 256 * 1024 * 1024;
        socket.set_recv_buffer_size(sock_buf_size)?;
        // Check
        let current_buf_size = socket.recv_buffer_size()?;
        if current_buf_size != sock_buf_size * 2 {
            return Err(Error::SetRecvBufferFailed {
                expected: sock_buf_size * 2,
                found: current_buf_size,
            }
            .into());
        }
        // Replace the socket2 socket with a std socket
        let sock = socket.into();
        Ok(Self {
            sock,
            buffer: [0u8; UDP_PAYLOAD],
            backlog: HashMap::with_capacity(BACKLOG_BUFFER_PAYLOADS),
            drops: 0,
            processed: 0,
            first_payload: true,
            oldest_count: 0,
        })
    }

    pub fn capture(&mut self) -> anyhow::Result<()> {
        let n = self.sock.recv(&mut self.buffer)?;
        if n != self.buffer.len() {
            Err(Error::SizeMismatch(n).into())
        } else {
            Ok(())
        }
    }

    pub fn capture_next_block(
        &mut self,
        mut slot: SendRef<'_, PayloadBlock>,
    ) -> anyhow::Result<(Duration, Duration)> {
        let n = slot.len();
        // Sneaky bit manipulation (all bits to 1 to set that the index corresponding with *that bit* needs to be filled)
        let mut to_fill = n - 1;

        let mut packet_time = Duration::default();

        // Fill every slot
        for _ in 0..n {
            // -- CAPTURE
            // Capture an arbitrary payload
            self.capture()?;
            // Time starts now to benchmark processing perf
            let now = Instant::now();
            // Decode its count
            let count = count(&self.buffer);
            if self.first_payload {
                self.oldest_count = count;
                self.first_payload = false;
            }
            // -- SORT
            // Find its position in this block
            if count < self.oldest_count {
                // Drop this payload, it happened in the past
                self.drops += 1;
            } else if count >= self.oldest_count + n as u64 {
                // Packet is destined for the future, insert into reorder buf
                self.backlog.insert(count, self.buffer);
            } else {
                let idx = (count - self.oldest_count) as usize;
                // Remove this idx from the `to_fill` entry
                to_fill &= !(1 << idx);
                // Packet is for this block! Insert into it's position
                slot[idx] = self.buffer;
                self.processed += 1;
            }
            // Stop the timer and add to the block time
            packet_time += now.elapsed();
        }
        // Now we'll fill in gaps with past data, if we have it
        // Otherwise replace with zeros and increment the drop count
        let block_process = Instant::now();
        for (idx, buf) in slot.iter_mut().enumerate() {
            // Check if this bit needs to be filled
            if (to_fill >> idx) & 1 == 1 {
                // Then either fill with data from the past, or set it as default
                let count = idx as u64 + self.oldest_count;
                if let Some(pl) = self.backlog.remove(&count) {
                    buf.clone_from_slice(&pl);
                    self.processed += 1;
                } else {
                    let mut pl = [0u8; UDP_PAYLOAD];
                    (pl[0..8]).clone_from_slice(&count.to_be_bytes());
                    buf.clone_from_slice(&pl);
                    self.drops += 1;
                }
            }
        }
        // Move the oldest count forward by the block size
        self.oldest_count += n as u64;
        let block_time = block_process.elapsed();
        Ok((packet_time, block_time))
    }
}

fn count(pl: &Payload) -> Count {
    u64::from_be_bytes(pl[0..8].try_into().unwrap())
}
