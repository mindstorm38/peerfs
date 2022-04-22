use std::net::{IpAddr, Ipv4Addr};
use std::time::Instant;
use std::env;

use peerfs::host::HostPeer;
use peerfs::pfs::PartialFile;


fn main() {

    const PARTIAL_FILE_TEST: &str = "C:/Users/theor/Downloads/partial_file_test";

    {
        let pf = PartialFile::create(PARTIAL_FILE_TEST, 123456).unwrap();
        println!("{:?}", pf);
    }

    {
        let pf = PartialFile::open(PARTIAL_FILE_TEST).unwrap();
        println!("{:?}", pf);
    }

    /*let mut peer0 = HostPeer::new(17127).unwrap();
    let mut peer1 = HostPeer::new(17128).unwrap();
    let mut peer2 = HostPeer::new(17129).unwrap();

    // Only peer1 know the other peer.
    peer1.add_peer(IpAddr::V4(Ipv4Addr::LOCALHOST), 17127);
    peer2.add_peer(IpAddr::V4(Ipv4Addr::LOCALHOST), 17128);

    let start = Instant::now();

    loop {
        peer0.tick();
        peer1.tick();
        peer2.tick();
        if start.elapsed().as_secs() > 2 {
            break;
        }
    }

    println!("[17127] {:?}", peer0.get_peers());
    println!("[17128] {:?}", peer1.get_peers());
    println!("[17129] {:?}", peer2.get_peers());*/

}
