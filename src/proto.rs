//! Protocol definition.
//! All peers must implement this protocol and are on the same level,
//! there is no notion of host and client.

use std::net::{Ipv4Addr, Ipv6Addr, IpAddr};
use std::io::{ErrorKind, Read, Write};
use std::io;

use byteorder::{ReadBytesExt, WriteBytesExt, BE};


const ID_REJECTED: u8           = 0xF0;

const ID_PEER_IDENTIFY: u8      = 0x01;
const ID_PEER_DISCOVER_IPV4: u8 = 0x02;
const ID_PEER_DISCOVER_IPV6: u8 = 0x03;
const ID_CHANNEL_OPEN: u8       = 0x10;
const ID_CHANNEL_HANDLE: u8     = 0x11;
const ID_FILE_OPEN: u8          = 0x20;
const ID_FILE_HANDLE: u8        = 0x21;
const ID_FILE_HANDLE_UPDATE: u8 = 0x22;
const ID_BLOCK_GET: u8          = 0x30;
const ID_BLOCK_DATA: u8         = 0x31;
const ID_BLOCK_CHECKSUM: u8     = 0x32;


/// A packet go from an "origin" peer to another "destination" peer.
#[derive(Debug)]
pub enum Packet {
    /// When an origin peer want to connect to destination peer and the destination
    /// peer reject the connection because the maximum capacity of peers has been
    /// reached.
    Rejected,
    /// After an origin peer has been TCP-linked to a destination peer, it send this
    /// packet to register itself.
    PeerIdentify {
        /// The port of the peer server to connect to.
        port: u16
    },
    /// This packet doesn't need a request to be accepted. But is triggered by
    /// `PeerIdentify`. It's used to discover other peers.
    PeerDiscover {
        addr: IpAddr,
        port: u16
    },
    ChannelOpen {
        request_id: u64,
        name: String
    },
    ChannelHandle {
        request_id: u64,
        handle: u64
    },
    /// A request to open a file, this works with the FILE_HANDLE packet to return
    /// the handle back to the requester, this will avoid huge path in request for
    /// file parts. The request ID is returned with the handle to ease traceability.
    FileOpen {
        request_id: u64,
        channel_handle: u64,
        path: String
    },
    /// A response to a FILE_OPEN packet, with the file handle and the request ID.
    /// This packet contains the list of currently supported block ranges.
    /// The peer receiving this packet can keep the information that the requesting
    /// node need the missing ranges, when the requested peer get some missing blocks
    /// of this file, it can update all peers that have previously requested this
    /// file using a FILE_HANDLE_UPDATE.
    FileHandle {
        request_id: u64,
        handle: u64,
        block_count: u64,
        block_ranges: Vec<(u64, u64)>
    },
    /// FILE_HANDLE_UPDATE (handle: u64, block_count: u64, block_ranges: Vec<(u64, u64)>)
    /// A packet sent to peers to update a previously request file with new supported
    /// block ranges or block count. This packet should be ignored if the peer hasn't
    /// previously requested the file, it should know the handle.
    FileHandleUpdate {
        handle: u64,
        block_count: u64,
        block_ranges: Vec<(u64, u64)>
    },
    /// A request to get a block from a file handle. The block size is currently
    /// assumed to be 4Kio. The request ID is used to follow the response.
    FileBlockGet {
        request_id: u64,
        handle: u64,
        index: u64
    },
    /// A response to FILE_BLOCK_GET with a block data, length is 4Kio if the block
    /// is fully used, if length is less than 4Kio this should be the last block.
    FileBlockData {
        request_id: u64,
        data: Vec<u8>
    },
    /// Fast check of the checksum of a file, used to validate a file on multiple
    /// nodes to ensure it was not (maybe intentionally) corrupted.
    /// Using fletcher 64bits.
    FileBlockChecksum {
        handle: u64,
        index: u64,
        checksum: u64
    }
}

impl Packet {

    pub fn read<R: Read>(mut read: R) -> io::Result<Packet> {

        let id = read.read_u8()?;

        match id {
            ID_REJECTED => {
                Ok(Packet::Rejected)
            }
            ID_PEER_IDENTIFY => {
                let port = read.read_u16::<BE>()?;
                Ok(Packet::PeerIdentify { port })
            }
            ID_PEER_DISCOVER_IPV4 => {
                let mut octets = [0; 4];
                read.read_exact(&mut octets[..])?;
                let addr = IpAddr::V4(Ipv4Addr::from(octets));
                let port = read.read_u16::<BE>()?;
                Ok(Packet::PeerDiscover { addr, port })
            }
            ID_PEER_DISCOVER_IPV6 => {
                let mut octets = [0; 16];
                read.read_exact(&mut octets[..])?;
                let addr = IpAddr::V6(Ipv6Addr::from(octets));
                let port = read.read_u16::<BE>()?;
                Ok(Packet::PeerDiscover { addr, port })
            }
            _ => Err(ErrorKind::InvalidData.into())
        }

    }

    pub fn write<W: Write>(&self, mut write: W) -> io::Result<()> {

        match self {
            Packet::Rejected => {
                write.write_u8(ID_REJECTED)?;
            }
            Packet::PeerIdentify { port } => {
                write.write_u8(ID_PEER_IDENTIFY)?;
                write.write_u16::<BE>(*port)?;
            }
            Packet::PeerDiscover { addr, port } => {
                match addr {
                    IpAddr::V4(v4) => {
                        write.write_u8(ID_PEER_DISCOVER_IPV4)?;
                        write.write_all(&v4.octets()[..])?;
                    }
                    IpAddr::V6(v6) => {
                        write.write_u8(ID_PEER_DISCOVER_IPV4)?;
                        write.write_all(&v6.octets()[..])?;
                    }
                }
                write.write_u16::<BE>(*port)?;
            }
            _ => unimplemented!()
        }

        Ok(())

    }

}