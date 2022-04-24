//!

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::time::Instant;
use std::path::Path;
use std::io::{self};
use std::rc::Rc;
use std::fmt;

use mio::net::TcpStream;

use crate::net::endpoint::{Endpoint, EndpointEvent, EndpointEvents, Link};
use crate::net::packet::Packet;
use crate::pfs::PartialFileSystem;


pub struct HostPeer {
    /// TODO
    endpoint: Endpoint,
    /// TODO
    endpoint_events: EndpointEvents,
    /// TODO
    endpoint_port: u16,
    /// Peers available to this peer.
    peers: Peers,
    /// Temporary testing pfs.
    pfs: PartialFileSystem,
}

impl HostPeer {

    pub fn new<P: AsRef<Path>>(port: u16, pfs_path: P) -> io::Result<Self> {

        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port);

        Ok(Self {
            endpoint: Endpoint::new(addr)?,
            endpoint_events: EndpointEvents::new(),
            endpoint_port: port,
            peers: Peers::new(),
            pfs: PartialFileSystem::new(pfs_path)?,
        })

    }

    pub fn get_peers(&self) -> &Peers {
        &self.peers
    }

    /// Manually add a known peer that can be used for filesystem exchange.
    pub fn add_peer(&mut self, addr: IpAddr, port: u16) {
        self.peers.add(addr, port, PeerStatus::Undefined);
    }

    pub fn tick(&mut self) -> io::Result<()> {

        self.peers.process_undefined_peers(|peer| {
            let peer_addr = peer.new_socket_addr();
            if let Ok(link) = self.endpoint.add_link_to(peer_addr) {
                link.send(&Packet::PeerIdentify { port: self.endpoint_port }).unwrap();
                peer.status = PeerStatus::Linked(Rc::clone(link));
            }
        });

        self.endpoint.poll(&mut self.endpoint_events)?;

        for event in self.endpoint_events.iter() {
            match event {
                EndpointEvent::NewLink(_link) => {

                }
                EndpointEvent::RejectedLink(link) => {
                    link.send(&Packet::Rejected).unwrap();
                }
                EndpointEvent::ReceivedPacket(link, addr, packet) => {
                    match packet {
                        Packet::Rejected => {
                            self.endpoint.remove_link(&**link).unwrap();
                        }
                        Packet::PeerIdentify { port } => {

                            let discover = Packet::PeerDiscover {
                                addr: addr.ip(),
                                port: *port
                            };

                            for peer in self.peers.iter() {

                                // We send this to the peer that sends us 'PeerIdentify'.
                                link.send(&Packet::PeerDiscover {
                                    addr: peer.addr,
                                    port: peer.port
                                }).unwrap();

                                // If the peer is currently linked, we send the identity.
                                if let PeerStatus::Linked(peer_link) = &peer.status {
                                    peer_link.send(&discover).unwrap();
                                }

                            }

                            self.peers.add(addr.ip(), *port, PeerStatus::Linked(Rc::clone(link)));

                        }
                        Packet::PeerDiscover { addr, port } => {
                            self.peers.add(*addr, *port, PeerStatus::Unlinked);
                        }
                        _ => {}
                    }
                }
            }
        }

        Ok(())

    }

}


/// Internally used to keep track of every peer known to this peer.
#[derive(Debug)]
pub struct Peers {
    peers: HashMap<(IpAddr, u16), Peer>,
    undefined_peers_count: usize
}

impl Peers {

    fn new() -> Self {
        Self {
            peers: HashMap::new(),
            undefined_peers_count: 0,
        }
    }

    fn add(&mut self, addr: IpAddr, port: u16, status: PeerStatus) {
        match self.peers.entry((addr, port)) {
            Entry::Occupied(mut o) => {
                let was_undefined = o.get().status.is_undefined();
                // Note that we can't downgrade from any status to undefined.
                o.get_mut().status.upgrade(status);
                if was_undefined && !o.get().status.is_undefined() {
                    self.undefined_peers_count -= 1;
                }
            }
            Entry::Vacant(v) => {
                if status.is_undefined() {
                    self.undefined_peers_count += 1;
                }
                v.insert(Peer::new(addr, port, status));
            }
        }
    }

    fn process_undefined_peers<P>(&mut self, mut predicate: P)
        where
            P: FnMut(&'_ mut Peer)
    {
        if self.undefined_peers_count != 0 {
            for peer in self.peers.values_mut() {
                let was_undefined = peer.status.is_undefined();
                (predicate)(peer);
                if was_undefined && !peer.status.is_undefined() {
                    self.undefined_peers_count -= 1;
                }
            }
        }
    }

    fn get_mut(&mut self, addr: IpAddr, port: u16) -> Option<&mut Peer> {
        self.peers.get_mut(&(addr, port))
    }

    pub fn iter(&self) -> impl Iterator<Item = &'_ Peer> + '_ {
        self.peers.values()
    }

    fn iter_mut(&mut self) -> impl Iterator<Item = &'_ mut Peer> + '_ {
        self.peers.values_mut()
    }

}

/// Internally used to track state of a remote peers.
#[derive(Debug)]
pub struct Peer {
    /// The remote address of the peer.
    pub addr: IpAddr,
    /// The remote port of the peer' server.
    pub port: u16,
    /// Is this peer currently linked?
    pub status: PeerStatus,
    /// Last active instant.
    pub last_active: Instant
}

impl Peer {

    fn new(addr: IpAddr, port: u16, status: PeerStatus) -> Self {
        Self {
            addr,
            port,
            status,
            last_active: Instant::now()
        }
    }

    fn new_socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.addr, self.port)
    }

    fn connect(&self) -> io::Result<TcpStream> {
        TcpStream::connect(self.new_socket_addr())
    }

}


#[derive(Clone)]
pub enum PeerStatus {
    /// Manually added peers are in a undefined state.
    Undefined,
    /// Such peers are not yet TCP-linked, but were discovered from other peers.
    Unlinked,
    /// TCP-linked peer.
    Linked(Rc<Link>),
}

impl PeerStatus {

    #[inline]
    pub fn is_undefined(&self) -> bool {
        matches!(self, PeerStatus::Undefined)
    }

    #[inline]
    pub fn is_unlinked(&self) -> bool {
        matches!(self, PeerStatus::Unlinked)
    }

    #[inline]
    pub fn is_linked(&self) -> bool {
        matches!(self, PeerStatus::Linked(_))
    }

    /// Only change the status if it is more advanced.
    pub fn upgrade(&mut self, other: PeerStatus) {
        match (self, other) {
            (PeerStatus::Unlinked, PeerStatus::Undefined) => { /* do nothing */ }
            (PeerStatus::Linked(_), PeerStatus::Undefined | PeerStatus::Unlinked) => { /* do nothing */ }
            (self_, other_) => { *self_ = other_; }
        }
    }

}

impl fmt::Debug for PeerStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PeerStatus::Undefined => f.write_str("Undefined"),
            PeerStatus::Unlinked => f.write_str("Unlinked"),
            PeerStatus::Linked(_) => f.write_str("Linked")
        }
    }
}
