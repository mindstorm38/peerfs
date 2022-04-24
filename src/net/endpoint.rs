//! Host endpoint.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use std::cell::RefCell;
use std::io::{self};
use std::rc::Rc;

use mio::{Events, Interest, Poll, Registry, Token};
use mio::net::{TcpListener, TcpStream};

use super::packet::Packet;

/// MIO token for when the server is ready to accept.
const TOK_SERVER_READY: Token = Token(0);
/// Maximum number of links to other peers' endpoints.
const MAX_LINK_COUNT: usize = 1024;


/// A peer to peer endpoint for the peerfs protocol.
pub struct Endpoint {
    /// The single server for the endpoint, accepting incoming connections from other peers.
    server: TcpListener,
    /// Peers connected to this peer.
    links: Links,
    /// Socket poll.
    poll: Poll,
    /// Socket events buffer for socket poll.
    events: Events,
}

impl Endpoint {

    pub fn new(addr: SocketAddr) -> io::Result<Self> {

        let mut tcp_listener = TcpListener::bind(addr)?;

        let poll = Poll::new()?;
        poll.registry().register(&mut tcp_listener, TOK_SERVER_READY, Interest::READABLE)?;

        Ok(Self {
            server: tcp_listener,
            links: Links::new(),
            poll,
            events: Events::with_capacity(1024)
        })

    }

    #[inline]
    pub fn get_links(&self) -> &Links {
        &self.links
    }

    /// Manually add a link to the given address.
    pub fn add_link_to(&mut self, addr: SocketAddr) -> io::Result<&Rc<Link>> {
        let link = self.links.link_to(addr)?;
        link.register(self.poll.registry(), Interest::READABLE)?;
        Ok(link)
    }

    /// Manually remove a link from the endpoint.
    pub fn remove_link(&mut self, link: &Link) -> io::Result<()> {
        link.deregister(self.poll.registry())
    }

    pub fn poll(&mut self, events: &mut EndpointEvents) -> io::Result<()> {

        events.clear();

        self.poll.poll(&mut self.events, Some(Duration::from_millis(50))).unwrap();

        for event in self.events.iter() {
            match event.token() {
                TOK_SERVER_READY => {

                    while let Ok((stream, addr)) = self.server.accept() {
                        match self.links.link(stream) {
                            Ok(link) => {
                                link.register(self.poll.registry(), Interest::READABLE).unwrap();
                                events.push(EndpointEvent::NewLink(Rc::clone(link)));
                            }
                            Err(link) => {
                                events.push(EndpointEvent::RejectedLink(link));
                            }
                        }
                    }

                }
                token => {

                    if let Some(link) = self.links.get(token) {
                        if let Some(addr) = link.peer_addr() {
                            while let Ok(packet) = link.recv() {
                                events.push(EndpointEvent::ReceivedPacket(Rc::clone(link), addr, packet));
                            }
                        }
                    }

                }
            }
        }

        Ok(())

    }

}


pub struct EndpointEvents {
    events: Vec<EndpointEvent>
}

impl EndpointEvents {

    pub fn new() -> Self {
        Self { events: Vec::new() }
    }

    #[inline]
    fn push(&mut self, event: EndpointEvent) {
        self.events.push(event);
    }

    #[inline]
    pub fn clear(&mut self) {
        self.events.clear();
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &'_ EndpointEvent> + '_ {
        self.events.iter()
    }

}


/// An event on the endpoint.
pub enum EndpointEvent {
    /// A successful new link.
    NewLink(Rc<Link>),
    /// A rejected link, the given link is not shared and its token isn't valid.
    /// You can use it to send packets for example.
    RejectedLink(Box<Link>),
    /// A packet has been received from the given link. We also give the peer
    /// address of the link.
    ReceivedPacket(Rc<Link>, SocketAddr, Packet),
}


/// A linked peer for a peerfs endpoint.
#[derive(Debug)]
pub struct Link {
    token: Token,
    stream: RefCell<TcpStream>,
}

impl Link {

    fn new(token: Token, stream: TcpStream) -> Self {
        Self {
            token,
            stream: RefCell::new(stream)
        }
    }

    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.stream.borrow().peer_addr().ok()
    }

    /// Internal method to register an interest for this link on a poll registry.
    fn register(&self, registry: &Registry, interest: Interest) -> io::Result<()> {
        registry.register(&mut *self.stream.borrow_mut(), self.token, interest)
    }

    /// Internal method to deregister an interest for this link on a poll registry.
    fn deregister(&self, registry: &Registry) -> io::Result<()> {
        registry.deregister(&mut *self.stream.borrow_mut())
    }

    /// Wait to receive a packet through this link.
    pub fn recv(&self) -> io::Result<Packet> {
        Packet::read(&mut *self.stream.borrow_mut())
    }

    /// Send a packet to through this link.
    pub fn send(&self, packet: &Packet) -> io::Result<()> {
        packet.write(&mut *self.stream.borrow_mut())
    }

}


/// Internally used to keep track of currently connected peers and their `TcpStream`.
pub struct Links {
    /// All TCP-linked peers.
    streams: HashMap<Token, Rc<Link>>,
    /// List of free tokens usable for event polling.
    free_tokens: Vec<Token>
}

impl Links {

    fn new() -> Self {
        Self {
            streams: HashMap::new(),
            free_tokens: (100usize..).take(MAX_LINK_COUNT).map(|i| Token(i)).collect()
        }
    }

    /// Try to link a peer, returning an error if no more peers can be linked.
    fn link(&mut self, stream: TcpStream) -> Result<&Rc<Link>, Box<Link>> {
        match self.free_tokens.pop() {
            Some(token) => {
                match self.streams.entry(token) {
                    Entry::Occupied(_) => panic!("streams map should not contain an entry for a free token"),
                    Entry::Vacant(v) => {
                        Ok(v.insert(Rc::new(Link::new(token, stream))))
                    }
                }
            }
            None => Err(Box::new(Link::new(Token(usize::MAX), stream)))
        }
    }

    fn link_to(&mut self, addr: SocketAddr) -> io::Result<&Rc<Link>> {
        self.link(TcpStream::connect(addr)?).map_err(|_| io::ErrorKind::Other.into())
    }

    fn unlink(&mut self, token: Token) -> Option<Rc<Link>> {
        match self.streams.remove(&token) {
            Some(link) => {
                self.free_tokens.push(token);
                Some(link)
            }
            None => None
        }
    }

    fn get(&mut self, token: Token) -> Option<&Rc<Link>> {
        self.streams.get(&token)
    }

}
