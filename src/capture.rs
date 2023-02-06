use socket2::{Domain, Socket, Type};
use std::net::UdpSocket;
use std::time::{Duration, Instant};
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};
use thingbuf::mpsc::blocking::SendRef;
use thiserror::Error;

use crate::{Count, Payload, BACKLOG_BUFFER_PAYLOADS, UDP_PAYLOAD};

#[derive(Error, Debug)]
/// Errors that can be produced from captures
pub enum Error {
    #[error("We recieved a payload which wasn't the size we expected {0}")]
    SizeMismatch(usize),
    #[error("Failed to set the recv buffer size. We tried to set {expected}, but found {found}. Check sysctl net.core.rmem_max")]
    SetRecvBufferFailed { expected: usize, found: usize },
}

pub struct Capture {
    sock: UdpSocket,
    buffer: Payload,
    pub backlog: HashMap<Count, Payload>,
    pub drops: usize,
    pub processed: usize,
    first_payload: bool,
    next_expected_count: Count,
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
        // Set the buffer size to 500MB (it will read as double, for some reason)
        let sock_buf_size = 256 * 1024 * 1024 * 2;
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
            next_expected_count: 0,
        })
    }

    pub fn capture(&mut self, buf: &mut Payload) -> anyhow::Result<()> {
        let n = self.sock.recv(buf)?;
        if n != self.buffer.len() {
            Err(Error::SizeMismatch(n).into())
        } else {
            Ok(())
        }
    }

    pub fn capture_sort(
        &mut self,
        mut slot: SendRef<'_, Box<Payload>>,
    ) -> anyhow::Result<Duration> {
        // By default, capture into the slot
        self.capture(&mut *slot)?;
        // Start the timer
        let now = Instant::now();
        // Then, we get the count
        let this_count = count(&*slot);
        // Check first payload
        if self.first_payload {
            self.first_payload = false;
            self.next_expected_count = this_count + 1;
            return Ok(now.elapsed());
        } else if this_count == self.next_expected_count {
            self.next_expected_count += 1;
            return Ok(now.elapsed());
        } else if this_count < self.next_expected_count {
            // If the packet is from the past, we drop it
            self.drops += 1;
        } else {
            // This packet is from the future, store it
            self.backlog.insert(this_count, **slot);
        }
        // If we got this far, this means we need to either replace the value of this slot with one from the backlog, or zeros
        if let Some(payload) = self.backlog.remove(&self.next_expected_count) {
            (**slot).clone_from(&payload);
        } else {
            // Nothing we can do, write zeros
            self.drops += 1;
        }
        Ok(now.elapsed())
    }
}

fn count(pl: &Payload) -> Count {
    u64::from_be_bytes(pl[0..8].try_into().unwrap())
}
