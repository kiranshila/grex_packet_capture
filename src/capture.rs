use socket2::{Domain, Socket, Type};
use std::net::UdpSocket;
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};
use thiserror::Error;

use crate::{Count, Payload, BACKLOG_BUFFER_PAYLOADS, UDP_PAYLOAD};

#[derive(Error, Debug)]
/// Errors that can be produced from captures
pub enum Error {
    #[error("We recieved a payload which wasn't the size we expected {0}")]
    SizeMismatch(usize),
}

pub struct Capture {
    pub sock: UdpSocket,
    pub buffer: Payload,
    pub backlog: HashMap<Count, Payload>,
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
        // Set the buffer size to 1GB
        let sock_buf_size = 256 * 1024 * 1024 * 4;
        socket.set_recv_buffer_size(sock_buf_size)?;
        // Set to nonblocking
        socket.set_nonblocking(true)?;
        // Replace the socket2 socket with a tokio socket
        let sock = socket.into();
        Ok(Self {
            sock,
            buffer: [0u8; UDP_PAYLOAD],
            backlog: HashMap::with_capacity(BACKLOG_BUFFER_PAYLOADS),
        })
    }

    pub fn capture(&mut self) -> anyhow::Result<()> {
        let n = loop {
            match self.sock.recv(&mut self.buffer) {
                Ok(n) => break n,
                Err(e) => {
                    if let Some(v) = e.raw_os_error() {
                        if v == 1 {
                            // EAGAIN
                            continue;
                        } else {
                            return Err(e.into());
                        }
                    } else {
                        return Err(e.into());
                    }
                }
            }
        };
        if n != self.buffer.len() {
            Err(Error::SizeMismatch(n).into())
        } else {
            Ok(())
        }
    }
}
