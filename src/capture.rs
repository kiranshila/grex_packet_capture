use socket2::{Domain, Socket, Type};
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::atomic::{AtomicBool, Ordering},
};
use thingbuf::mpsc::Sender;
use thingbuf::Recycle;
use thiserror::Error;
use tokio::net::UdpSocket;

/// UDP size set by the FPGA gateware
const UDP_PAYLOAD: usize = 8200;
const BACKLOG_BUFFER_PAYLOADS: usize = 1024; // It should never exceed this

type Count = u64;
pub type PayloadBytes = [u8; UDP_PAYLOAD];
type ChannelPayload = Box<PayloadBytes>;

pub fn boxed_payload() -> ChannelPayload {
    Box::new([0u8; UDP_PAYLOAD])
}

pub struct PayloadRecycle;

impl PayloadRecycle {
    pub const fn new() -> Self {
        Self
    }
}

impl Recycle<ChannelPayload> for PayloadRecycle {
    fn new_element(&self) -> ChannelPayload {
        Box::new([0u8; UDP_PAYLOAD])
    }

    fn recycle(&self, _: &mut ChannelPayload) {
        // Do nothing, we will write to every position anyway
    }
}

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
    pub backlog: HashMap<Count, PayloadBytes>,
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
        // Replace the socket2 socket with a tokio socket
        let sock = UdpSocket::from_std(socket.into())?;
        Ok(Self {
            sock,
            backlog: HashMap::with_capacity(BACKLOG_BUFFER_PAYLOADS),
            drops: 0,
            processed: 0,
            first_payload: true,
            next_expected_count: 0,
        })
    }

    pub async fn capture(&mut self, buf: &mut PayloadBytes) -> anyhow::Result<()> {
        let n = self.sock.recv(buf).await?;
        if n != buf.len() {
            Err(Error::SizeMismatch(n).into())
        } else {
            Ok(())
        }
    }

    pub async fn start(
        &mut self,
        payload_sender: Sender<ChannelPayload, PayloadRecycle>,
        shutdown: &AtomicBool,
    ) -> anyhow::Result<()> {
        loop {
            if shutdown.load(Ordering::Acquire) {
                break;
            }
            // Grab the next slot
            let mut slot = payload_sender.send_ref().await?;
            // By default, capture into the slot
            self.capture(&mut *slot).await?;
            self.processed += 1;
            // Then, we get the count
            let this_count = count(&*slot);
            // Check first payload
            if self.first_payload {
                self.first_payload = false;
                self.next_expected_count = this_count + 1;
                continue;
            } else if this_count == self.next_expected_count {
                self.next_expected_count += 1;
                continue;
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
                (**slot).clone_from(&[0u8; UDP_PAYLOAD]);
                (**slot)[0..8].clone_from_slice(&this_count.to_be_bytes());
                self.drops += 1;
            }
        }
        Ok(())
    }
}

fn count(pl: &PayloadBytes) -> Count {
    u64::from_be_bytes(pl[0..8].try_into().unwrap())
}
