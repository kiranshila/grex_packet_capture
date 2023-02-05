use socket2::{Domain, Socket, Type};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use thiserror::Error;
use tokio::net::UdpSocket;

#[derive(Error, Debug)]
/// Errors that can be produced from captures
pub enum Error {
    #[error("We recieved a payload which wasn't the size we expected {0}")]
    SizeMismatch(usize),
}

pub struct Capture {
    pub sock: UdpSocket,
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
        Ok(Self { sock })
    }

    pub async fn capture(&mut self, buf: &mut [u8]) -> anyhow::Result<()> {
        let n = self.sock.recv(buf).await?;
        if n != buf.len() {
            Err(Error::SizeMismatch(n).into())
        } else {
            Ok(())
        }
    }
}
