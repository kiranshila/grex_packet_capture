use anyhow::{bail, Result};
use itertools::Itertools;
use libc::{c_void, iovec, mmsghdr, msghdr, recvmmsg};
use socket2::{Domain, Socket, Type};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    os::unix::prelude::AsRawFd,
    ptr::null_mut,
};

const RMEM_MAX: usize = 2097152;

struct BulkUdpCapture {
    sock: Socket,
    msgs: Vec<mmsghdr>,
    buffers: Vec<Vec<u8>>,
    _iovecs: Vec<iovec>,
}

impl BulkUdpCapture {
    pub fn new(port: u16, packets_per_capture: usize, packet_size: usize) -> Result<Self> {
        // Create the bog-standard UDP socket
        let sock = Socket::new(Domain::IPV4, Type::DGRAM, None)?;
        // Create its local address and bind
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port);
        sock.bind(&addr.into())?;
        // Make the recieve buffer huge
        sock.set_recv_buffer_size(RMEM_MAX)?;
        // Create the arrays on the heap to point the NIC to
        let mut buffers = vec![vec![0u8; packet_size + 1]; packets_per_capture];
        // And connect up the scatter-gather buffers
        let mut iovecs: Vec<_> = buffers
            .iter_mut()
            .map(|ptr| iovec {
                iov_base: ptr.as_mut_ptr() as *mut c_void,
                iov_len: packet_size,
            })
            .collect();
        let msgs: Vec<_> = iovecs
            .iter_mut()
            .map(|ptr| mmsghdr {
                msg_hdr: msghdr {
                    msg_name: null_mut(),
                    msg_namelen: 0,
                    msg_iov: ptr as *mut iovec,
                    msg_iovlen: 1,
                    msg_control: null_mut(),
                    msg_controllen: 0,
                    msg_flags: 0,
                },
                msg_len: 0,
            })
            .collect();
        Ok(Self {
            sock,
            msgs,
            buffers,
            _iovecs: iovecs,
        })
    }

    pub fn capture(&mut self) -> Result<&[Vec<u8>]> {
        let captured = unsafe {
            recvmmsg(
                self.sock.as_raw_fd(),
                self.msgs.as_mut_ptr(),
                self.buffers.len().try_into().unwrap(),
                0,
                null_mut(),
            )
        };
        if captured == -1 {
            bail!("Bad capture");
        }
        if captured as usize != self.buffers.len() {
            bail!("Didn't recieve enough packets");
        }
        Ok(&self.buffers[..captured as usize])
    }
}

const ITERS: usize = 512; // ~4 million packets
const PACKETS: usize = 8192;

fn main() -> anyhow::Result<()> {
    let mut counts = vec![0u64; ITERS * PACKETS];
    let mut cap = BulkUdpCapture::new(60000, 8192, 8200)?;
    for i in 0..ITERS {
        for (j, payload) in cap.capture()?.iter().enumerate() {
            counts[i * ITERS + j] = u64::from_be_bytes(payload[..8].try_into().unwrap());
        }
    }
    // And process
    println!("Captured {} packets!", counts.len());
    counts.sort();
    let mut deltas: Vec<_> = counts.windows(2).map(|v| v[1] - v[0]).collect();
    deltas.sort();
    dbg!(deltas.iter().dedup_with_count().collect::<Vec<_>>());
    Ok(())
}
