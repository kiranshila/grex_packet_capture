use std::{
    collections::HashMap,
    mem::MaybeUninit,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::{Duration, Instant},
};

use socket2::{Domain, Socket, Type};
use thingbuf::Recycle;
use thiserror::Error;
use tokio::net::UdpSocket;

const UDP_PAYLOAD: usize = 8200;
const BACKLOG_BUFFER_PAYLOADS: usize = 4096;
const BLOCK_PAYLOAD_POW: u32 = 15;
const BLOCK_PAYLOADS: usize = 2usize.pow(BLOCK_PAYLOAD_POW);

#[derive(Debug)]
/// Capture object to grab observation data from the network card
pub struct Capture {
    /// The socket fd itself
    sock: UdpSocket,
    /// A reusable container to capture into
    buf: PayloadBytes,
    /// The oldest count for the current block
    oldest_count: Count,
    /// A flag to indicate if we've captured our first packet
    first_packet: bool,
    /// The number of packets we've dropped
    pub drops: usize,
    /// The number of packets we've processed
    pub processed: usize,
    /// The hashmap that will store futuristic payloads
    backlog: HashMap<Count, PayloadBytes>,
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

        // Replace the socket2 socket with a tokio socket
        let sock = UdpSocket::from_std(socket.into())?;

        Ok(Self {
            sock,
            buf: PayloadBytes::default(),
            oldest_count: 0,
            first_packet: false,
            backlog: HashMap::with_capacity(BACKLOG_BUFFER_PAYLOADS),
            drops: 0,
            processed: 0,
        })
    }

    /// Side effect by capture a block of bytes from the network card and updating our internal buffer
    ///
    /// # Errors
    /// Returns an error on socket IO errors and size mismatch
    pub async fn capture(&mut self) -> anyhow::Result<()> {
        let n = self.sock.recv(&mut self.buf.0).await?;
        if n != self.buf.0.len() {
            Err(Error::SizeMismatch(n).into())
        } else {
            Ok(())
        }
    }

    /// Capture observations from the connected socket and fill the block buffer such that they are sorted.
    /// Returns (Packet processing time, block processing time)
    ///
    /// Note: This is stateful and will keep blocks sorted across calls, including using internal backlog state
    /// to deal with out-of-order packets
    pub async fn capture_sorted_block(
        &mut self,
        block_buffer: &mut PayloadBlock,
    ) -> anyhow::Result<(Duration, Duration)> {
        // Sneaky bit manipulation (all bits to 1 to set that the index corresponding with *that bit* needs to be filled)
        let mut to_fill = block_buffer.0.len() - 1;
        let mut packet_time = Duration::default();
        for _ in 0..block_buffer.0.len() {
            // ----- Capture phase
            // Capture the next payload
            self.capture().await?;
            // Start the packet timer
            let packet_start_time = Instant::now();
            // Decode the count
            let count = self.buf.count();
            // Deal with the first packet
            if self.first_packet {
                self.oldest_count = count;
                self.first_packet = false;
            }
            // ----- Sorting phase
            // Find this payload's position in the block
            if count < self.oldest_count {
                // Drop this payload, it happened in the past
                self.drops += 1;
            } else if count >= self.oldest_count + block_buffer.0.len() as u64 {
                // Packet is destined for the future, insert into reorder buf
                self.backlog.insert(count, self.buf);
            } else {
                let idx = (count - self.oldest_count) as usize;
                // Remove this idx from the `to_fill` entry
                to_fill &= !(1 << idx);
                // Packet is for this block! Insert into it's position
                block_buffer.0[idx].write(self.buf);
                self.processed += 1;
            }
            // Stop the packet timer
            packet_time += packet_start_time.elapsed();
        }
        // Start the block timer
        let block_time = Instant::now();
        // Once we've captured a packet for each spot in the buffer, fill in the missing spots
        for (idx, buf) in block_buffer.0.iter_mut().enumerate() {
            // Check if this bit needs to be filled
            if (to_fill >> idx) & 1 == 1 {
                // Then either fill with data from the past, or set it as default
                let count = idx as u64 + self.oldest_count;
                if let Some(pl) = self.backlog.remove(&count) {
                    buf.write(pl);
                    self.processed += 1;
                } else {
                    buf.write(PayloadBytes::default());
                    self.drops += 1;
                }
            }
        }
        // Move our oldest count ahead by the block size
        self.oldest_count += block_buffer.0.len() as u64;
        Ok((
            packet_time / block_buffer.0.len().try_into().unwrap(),
            block_time.elapsed(),
        ))
    }
}

#[derive(Error, Debug)]
/// Errors that can be produced from captures
pub enum Error {
    #[error("We recieved a payload which wasn't the size we expected {0}")]
    SizeMismatch(usize),
}

/// The size of a payload count (set by the FPGA)
type Count = u64;

#[derive(Debug, Clone, Copy)]
/// Newtype wrapper for one observation of data
pub struct PayloadBytes([u8; UDP_PAYLOAD]);

impl PayloadBytes {
    /// Decode the count header for this payload
    fn count(&self) -> Count {
        u64::from_be_bytes(self.0[0..8].try_into().unwrap())
    }
}

impl Default for PayloadBytes {
    fn default() -> Self {
        Self([0u8; UDP_PAYLOAD])
    }
}

#[derive(Clone)]
/// A block of multiple payloads that we will pass around via channels.
/// Data is MaybeUninit as it's not valid until it's populated from the capture task.
/// After that point, it is assumed to be valid.
pub struct PayloadBlock([MaybeUninit<PayloadBytes>; BLOCK_PAYLOADS]);

/// Zero-size struct for our custom Recycle impl that does nothing on recycle to preserve the uninit and avoid allocating
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
