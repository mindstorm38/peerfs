use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::collections::hash_map::Entry;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;
use std::fmt;
use std::io;

use mio::net::{TcpListener, TcpStream};
use mio::{Poll, Events, Token, Interest, Registry};

use crate::net::packet::Packet;
use crate::pfs::PartialFileSystem;


const TOK_SERVER_READY: Token = Token(0);
const MAX_PEER_COUNT: usize = 1024;


/// A peer on a network, sending and receiving packets from other peers.
pub struct HostPeer {
    /// The single server of this peer, accepting incoming connections from other peers.
    server: TcpListener,
    /// TCP port the server is bound to.
    server_port: u16,
    /// Peers connected to this peer.
    linked_peers: LinkedPeers,
    /// Peers available to this peer.
    peers: Peers,

    /// Temporary testing pfs.
    pfs: PartialFileSystem,

    poll: Poll,
    events: Events,
}

impl HostPeer {

    /// Creates a new Peer.
    pub fn new<P: AsRef<Path>>(port: u16, pfs_path: P) -> io::Result<Self> {

        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port);
        let mut tcp_listener = TcpListener::bind(addr).unwrap();

        let poll = Poll::new()?;
        poll.registry().register(&mut tcp_listener, TOK_SERVER_READY, Interest::READABLE).unwrap();

        Ok(Self {
            server: tcp_listener,
            server_port: port,
            linked_peers: LinkedPeers::new(),
            peers: Peers::new(),
            pfs: PartialFileSystem::new(pfs_path).unwrap(),
            poll,
            events: Events::with_capacity(1024)
        })

    }

    pub fn get_linked_peers(&self) -> &LinkedPeers {
        &self.linked_peers
    }

    pub fn get_peers(&self) -> &Peers {
        &self.peers
    }

    /// Manually add a known peer that can be used for filesystem exchange.
    pub fn add_peer(&mut self, addr: IpAddr, port: u16) {
        self.peers.add(addr, port, PeerStatus::Undefined);
    }

    pub fn tick(&mut self) {

        self.peers.process_undefined_peers(|peer| {
            let peer_addr = peer.new_socket_addr();
            println!("[{:05}] [SEND] Peer {} is not linked, trying...", self.server_port, peer_addr);
            match self.linked_peers.link_addr(peer_addr) {
                Ok(linked_peer) => {
                    println!("               Link success, registering to poll...");
                    linked_peer.register(self.poll.registry(), Interest::READABLE);
                    linked_peer.send(&Packet::PeerIdentify { port: self.server_port }).unwrap();
                    peer.status = PeerStatus::Linked(Rc::clone(linked_peer));
                }
                Err(err) => {
                    println!("             Failed to link peer: {:?}", err);
                }
            }
        });

        self.poll.poll(&mut self.events, Some(Duration::from_millis(50))).unwrap();

        for event in self.events.iter() {
            match event.token() {
                TOK_SERVER_READY => {

                    while let Ok((stream, addr)) = self.server.accept() {
                        println!("[{:05}] [RECV] Accepted peer connection from {}, try linking...", self.server_port, addr);
                        match self.linked_peers.link(stream) {
                            Ok(linked_peer) => {
                                println!("               Link success, registering to poll...");
                                linked_peer.register(self.poll.registry(), Interest::READABLE);
                            }
                            Err(stream) => {
                                println!("               Link failed, no space available, rejecting...");
                                Packet::Rejected.write(stream).unwrap();
                            }
                        }
                    }

                }
                token => {

                    // Indicate if we need to unlink the received stream.
                    let mut need_unlink = false;
                    // Vector of newly identified peers to be discovered by another peers.
                    let mut new_peers = Vec::new();

                    if let Some(linked_peer) = self.linked_peers.get(token) {
                        if let Some(linked_addr) = linked_peer.peer_addr() {

                            println!("[{:05}] [RECV] Receiving from {}...", self.server_port, linked_addr);

                            while let Ok(packet) = linked_peer.recv() {
                                println!("             - {:?}", packet);
                                match packet {
                                    Packet::Rejected => {
                                        need_unlink = true;
                                    }
                                    Packet::PeerIdentify { port } => {

                                        // It is linked because it just sent this packet to us.
                                        let ip = linked_addr.ip();
                                        self.peers.add(ip, port, PeerStatus::Linked(Rc::clone(linked_peer)));

                                        for peer in self.peers.iter() {
                                            if peer.addr != ip || peer.port != port {
                                                linked_peer.send(&Packet::PeerDiscover {
                                                    addr: peer.addr,
                                                    port: peer.port
                                                }).unwrap();
                                            }
                                        }

                                        new_peers.push((ip, port));

                                    }
                                    Packet::PeerDiscover { addr, port } => {
                                        self.peers.add(addr, port, PeerStatus::Unlinked);
                                    }
                                    Packet::FileOpen { request_id, channel_handle: _, path } => {
                                        self.pfs.open(path)
                                    }
                                    _ => {}
                                }
                            }

                        }
                    }

                    if need_unlink {
                        if let Some(linked_peer) = self.linked_peers.unlink(token) {
                            linked_peer.deregister(self.poll.registry());
                        }
                    }

                    for (ip, port) in new_peers {

                        let packet = Packet::PeerDiscover {
                            addr: ip,
                            port
                        };

                        for peer in self.peers.iter() {
                            if let PeerStatus::Linked(ref token) = peer.status {
                                // We don't want to send the discover packet to the peer itself.
                                if peer.addr != ip || peer.port != port {
                                    token.send(&packet).unwrap();
                                }
                            }
                        }

                    }

                }
            }
        }

    }

}


#[derive(Debug)]
pub struct LinkedPeer {
    token: Token,
    stream: RefCell<TcpStream>,
}

impl LinkedPeer {

    fn new(token: Token, stream: TcpStream) -> Self {
        Self {
            token,
            stream: RefCell::new(stream)
        }
    }

    fn peer_addr(&self) -> Option<SocketAddr> {
        self.stream.borrow().peer_addr().ok()
    }

    fn register(&self, registry: &Registry, interest: Interest) {
        registry.register(&mut *self.stream.borrow_mut(), self.token, interest).unwrap();
    }

    fn deregister(&self, registry: &Registry) {
        registry.deregister(&mut *self.stream.borrow_mut()).unwrap();
    }

    fn recv(&self) -> io::Result<Packet> {
        Packet::read(&mut *self.stream.borrow_mut())
    }

    fn send(&self, packet: &Packet) -> io::Result<()> {
        packet.write(&mut *self.stream.borrow_mut())
    }

}


/// Internally used to keep track of currently connected peers and their `TcpStream`.
pub struct LinkedPeers {
    /// All TCP-linked peers.
    streams: HashMap<Token, Rc<LinkedPeer>>,
    /// List of free tokens usable for event polling.
    free_tokens: Vec<Token>
}

impl LinkedPeers {

    fn new() -> Self {
        Self {
            streams: HashMap::new(),
            free_tokens: (100usize..).take(MAX_PEER_COUNT).map(|i| Token(i)).collect()
        }
    }

    /// Try to link a peer, returning an error if no more peers can be linked.
    fn link(&mut self, stream: TcpStream) -> Result<&Rc<LinkedPeer>, TcpStream> {
        match self.free_tokens.pop() {
            Some(token) => {
                match self.streams.entry(token) {
                    Entry::Occupied(_) => panic!("streams map should not contain an entry for a free token"),
                    Entry::Vacant(v) => {
                        Ok(v.insert(Rc::new(LinkedPeer::new(token, stream))))
                    }
                }
            }
            None => Err(stream)
        }
    }

    fn link_addr(&mut self, addr: SocketAddr) -> io::Result<&Rc<LinkedPeer>> {
        self.link(TcpStream::connect(addr)?).map_err(|_| io::ErrorKind::Other.into())
    }

    fn unlink(&mut self, token: Token) -> Option<Rc<LinkedPeer>> {
        match self.streams.remove(&token) {
            Some(link) => {
                self.free_tokens.push(token);
                Some(link)
            }
            None => None
        }
    }

    fn get(&mut self, token: Token) -> Option<&Rc<LinkedPeer>> {
        self.streams.get(&token)
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
    Linked(Rc<LinkedPeer>),
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