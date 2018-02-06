extern crate env_logger;
extern crate getopts;
#[macro_use]
extern crate log;
extern crate mio;
extern crate rand;

use rand::Rng;
use std::collections::BinaryHeap;
use std::collections::VecDeque;
use std::io::Read;
use std::io::Write;

mod multiplexer_tests;

trait VecDequeUtil {
    fn push_back_usize(&mut self, value: usize, length: usize);
    fn peek_usize(&self, position: usize, length: usize) -> usize;
}

impl VecDequeUtil for VecDeque<u8> {
    fn push_back_usize(&mut self, value: usize, length: usize) {
        assert!(
            length * 8 < usize::max_value().count_ones() as usize,
            "length is too big for usize (actual: {}, expected: _ < {}).",
            length * 8,
            usize::max_value().count_ones()
        );
        let mut value = value;
        for _ in 0..length {
            self.push_back(value as u8);
            value >>= 8;
        }
    }

    fn peek_usize(&self, start: usize, length: usize) -> usize {
        assert!(
            length * 8 < usize::max_value().count_ones() as usize,
            "length is too big for usize (actual: {}, expected: _ < {}).",
            length * 8,
            usize::max_value().count_ones()
        );
        assert!(
            start + length <= self.len(),
            "Out of bounds in peek_usize (actual: {}, expected: _ <= {}).",
            start + length,
            self.len()
        );

        let mut result = 0;
        let mut base = 1;
        for position in start..(start + length) {
            result += self[position] as usize * base;
            base <<= 8;
        }
        result
    }
}

trait ErrorUtil {
    fn prepend_err(self, message: &str) -> Self;
}

impl<T> ErrorUtil for std::io::Result<T> {
    fn prepend_err(self, message: &str) -> Self {
        self.map_err(|error| {
            std::io::Error::new(error.kind(), format!("{}\n- {}", message, error))
        })
    }
}

#[derive(Clone, Debug)]
struct MultiplexerStats {
    time: std::time::Instant,
    flush: usize,
    master_read: usize,
    master_read_packets: usize,
    master_write: usize,
    master_write_packets: usize,
}

impl MultiplexerStats {
    fn new(time: std::time::Instant) -> Self {
        Self {
            time: time,
            flush: 0,
            master_read: 0,
            master_read_packets: 0,
            master_write: 0,
            master_write_packets: 0,
        }
    }
}

pub struct Multiplexer {
    /// Name of the multiplexer connection.  Used for debugging.
    name: String,
    /// Read buffers, each of which is bound to a slave connection.
    pub(crate) slave_read_buffers: Vec<VecDeque<u8>>,
    /// Write buffers, each of which is bound to a slave connection.
    pub(crate) slave_write_buffers: Vec<VecDeque<u8>>,
    /// Priority queue of (capacity, index).
    pub(crate) slave_write_capacity_queue: BinaryHeap<(usize, usize)>,
    /// Connectivity for each slave.
    pub(crate) slave_connectivities: Vec<bool>,
    /// A read buffer for the master connection.
    pub(crate) master_read_buffer: VecDeque<u8>,
    /// Write buffers, each of which represents a pair of existence and a chunk.
    pub(crate) master_write_buffers: VecDeque<(bool, Vec<u8>)>,
    /// Master's connectivity.
    pub(crate) master_connectivity: bool,
    /// # of alive connections.
    pub(crate) num_of_alive_slaves: usize,
    /// Chunk index to read from slaves.
    pub(crate) gather_index: usize,
    /// Chunk index to write to slaves.
    pub(crate) scatter_index: usize,
    /// Is the state changed.
    pub(crate) is_dirty: bool,
    /// Temporary buffer.
    temporary_buffer: [u8; 1024],
    /// Stats.
    stats: MultiplexerStats,
    /// Stats which are output in the last time.
    last_stats: MultiplexerStats,
}

impl Multiplexer {
    const SLAVE_BUFFER_SIZE: usize = 16 * 1024;
    const MASTER_BUFFER_SIZE: usize = 128 * 1024;
    const PACKET_SIZE: usize = 1024;

    const INDEX_HEADER_SIZE: usize = 4;
    const LENGTH_HEADER_SIZE: usize = 2;
    const HEADER_SIZE: usize = Self::INDEX_HEADER_SIZE + Self::LENGTH_HEADER_SIZE;

    pub fn new(name: &str, num_of_slaves: usize) -> Multiplexer {
        info!(
            "Multiplexer {}: It will allocate {} bytes for buffering.",
            name,
            Self::SLAVE_BUFFER_SIZE * num_of_slaves * 2 + Self::MASTER_BUFFER_SIZE * 2
        );

        // Assert constants.
        assert!(
            Self::PACKET_SIZE.count_ones() == 1,
            "PACKET_SIZE must be a power of 2, but {}.",
            Self::PACKET_SIZE
        );
        assert!(
            Self::MASTER_BUFFER_SIZE % Self::PACKET_SIZE == 0,
            "MASTER_BUFFER_SIZE must be divisible by PACKET_SIZE, but the remainder is {}.",
            Self::MASTER_BUFFER_SIZE % Self::PACKET_SIZE
        );

        // Prepare a skeleton of Multiplexer.
        let current_time = std::time::Instant::now();
        let mut multiplexer = Multiplexer {
            name: name.into(),
            slave_read_buffers: Vec::with_capacity(num_of_slaves),
            slave_write_buffers: Vec::with_capacity(num_of_slaves),
            slave_write_capacity_queue: BinaryHeap::with_capacity(num_of_slaves),
            slave_connectivities: Vec::with_capacity(num_of_slaves),
            master_read_buffer: VecDeque::with_capacity(Self::MASTER_BUFFER_SIZE),
            master_write_buffers: VecDeque::with_capacity(
                Self::MASTER_BUFFER_SIZE / Self::PACKET_SIZE,
            ),
            master_connectivity: true,
            num_of_alive_slaves: num_of_slaves,
            gather_index: 0,
            scatter_index: 0,
            is_dirty: true,
            temporary_buffer: [0; Self::PACKET_SIZE],
            stats: MultiplexerStats::new(current_time),
            last_stats: MultiplexerStats::new(current_time),
        };

        // Populate 2-dimensional vectors.
        for _ in 0..num_of_slaves {
            multiplexer
                .slave_read_buffers
                .push(VecDeque::with_capacity(Self::SLAVE_BUFFER_SIZE));
            multiplexer
                .slave_write_buffers
                .push(VecDeque::with_capacity(Self::SLAVE_BUFFER_SIZE));
            multiplexer.slave_connectivities.push(true);
        }
        while multiplexer.master_write_buffers.len() < multiplexer.master_write_buffers.capacity() {
            multiplexer
                .master_write_buffers
                .push_back((false, Vec::with_capacity(Self::PACKET_SIZE)));
        }

        multiplexer
    }

    pub fn read_master<Reader: std::io::Read + mio::Evented>(
        &mut self,
        master: &mut Reader,
    ) -> std::io::Result<()> {
        let master_read_buffer = &mut self.master_read_buffer;
        while master_read_buffer.len() < master_read_buffer.capacity() {
            let length_limit = std::cmp::min(
                master_read_buffer.capacity() - master_read_buffer.len(),
                self.temporary_buffer.len(),
            );
            let length = match master.read(&mut self.temporary_buffer[0..length_limit]) {
                Ok(0) => {
                    if self.master_connectivity {
                        info!("Multiplexer {}: Connection to master is closed.", self.name);
                        self.master_connectivity = false;
                    }
                    break;
                }
                Ok(length) => length,
                Err(ref error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) => {
                    return Err(error).prepend_err(&format!(
                        "Failed to try reading {} bytes from a master.",
                        length_limit
                    ))
                }
            };
            self.is_dirty = true;
            self.stats.master_read += length;
            self.stats.master_read_packets += 1;
            self.temporary_buffer[..length].iter().for_each(|byte| {
                master_read_buffer.push_back(*byte);
            });
            assert!(
                master_read_buffer.capacity() <= Self::MASTER_BUFFER_SIZE * 2,
                "Multiplexer {}: master_read_buffer's capacity is too big \
                 (actual: {}, expected: _ <= {}).",
                self.name,
                master_read_buffer.capacity(),
                Self::MASTER_BUFFER_SIZE * 2
            );
        }

        Ok(())
    }

    pub fn write_master<Writer: std::io::Write + mio::Evented>(
        &mut self,
        master: &mut Writer,
    ) -> std::io::Result<()> {
        loop {
            // If consuming the current buffer completes, advance the gather index.
            if {
                let (existence, ref mut master_write_buffer) = self.master_write_buffers[0];

                // Finish if the next buffer is unavailable.
                if !existence {
                    break;
                }

                // Write data and remove corresponding ones from the buffer.
                let length = match master.write(master_write_buffer.as_slice()) {
                    Ok(length) => length,
                    Err(ref error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(error) => {
                        return Err(error).prepend_err(&format!(
                            "Failed to write {} bytes to a master socket.",
                            master_write_buffer.len()
                        ))
                    }
                };
                self.is_dirty = true;
                self.stats.master_write += length;
                self.stats.master_write_packets += 1;
                master_write_buffer.drain(..length);
                master_write_buffer.is_empty()
            } {
                // Advance master_write_buffers along with gather_index increment.
                let master_write_buffer = match self.master_write_buffers.pop_front() {
                    Some((_, master_write_buffer)) => master_write_buffer,
                    _ => unreachable!(),
                };
                assert!(
                    master_write_buffer.capacity() == Self::PACKET_SIZE,
                    "Multiplexer {}: Unexpected capacity for a master write buffer \
                     (actual: {}, expected: {}).",
                    self.name,
                    master_write_buffer.capacity(),
                    Self::PACKET_SIZE
                );
                self.master_write_buffers
                    .push_back((false, master_write_buffer));
                self.gather_index += 1;
            }
        }

        Ok(())
    }

    pub fn read_slaves<Reader: std::marker::Sized + std::io::Read + mio::Evented>(
        &mut self,
        slaves: &mut Vec<Reader>,
    ) -> std::io::Result<()> {
        assert!(
            self.slave_read_buffers.len() == slaves.len(),
            "Multiplexer {}: Inconsistent number of slaves (expected: {}, actual: {}).",
            self.name,
            self.slave_read_buffers.len(),
            slaves.len()
        );

        // For each slave, receive bytes from the slave and store them into
        // a buffer as much as possible.
        for (index, slave_read_buffer) in self.slave_read_buffers.iter_mut().enumerate() {
            while slave_read_buffer.len() < slave_read_buffer.capacity() {
                let length = std::cmp::min(
                    self.temporary_buffer.len(),
                    slave_read_buffer.capacity() - slave_read_buffer.len(),
                );
                let length = match slaves[index].read(&mut self.temporary_buffer[..length]) {
                    Ok(0) => {
                        if self.slave_connectivities[index] {
                            info!(
                                "Multiplexer {}: Connection to slave #{} is dead.",
                                self.name,
                                index
                            );
                            self.slave_connectivities[index] = false;
                            self.num_of_alive_slaves -= 1;
                        }
                        break;
                    }
                    Ok(length) => length,
                    Err(ref error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(error) => {
                        return Err(error).prepend_err(&format!(
                            "Failed to read {} bytes from slave #{}.",
                            length,
                            index
                        ));
                    }
                };
                self.is_dirty = true;
                self.temporary_buffer[..length].iter().for_each(|byte| {
                    slave_read_buffer.push_back(*byte);
                });
                assert!(
                    slave_read_buffer.capacity() < Self::SLAVE_BUFFER_SIZE * 2,
                    "Multiplexer {}: slave_read_buffer's capacity is too big \
                     (actual: {}, expected: _ < {}).",
                    self.name,
                    slave_read_buffer.capacity(),
                    Self::SLAVE_BUFFER_SIZE * 2
                );
            }
        }

        Ok(())
    }

    pub fn write_slaves<Writer: std::marker::Sized + std::io::Write + mio::Evented>(
        &mut self,
        slaves: &mut Vec<Writer>,
    ) -> std::io::Result<()> {
        assert!(
            self.slave_write_buffers.len() == slaves.len(),
            "Multiplexer {}: Inconsistent number of slaves (expected: {}, actual: {}).",
            self.name,
            self.slave_write_buffers.len(),
            slaves.len()
        );

        // For each slave, write the contents of a buffer into the slave as much as possible.
        for (index, slave_write_buffer) in self.slave_write_buffers.iter_mut().enumerate() {
            while !slave_write_buffer.is_empty() {
                assert!(
                    !slave_write_buffer.as_slices().0.is_empty(),
                    "VecDequeue.as_slices().0 is not expected to be empty."
                );
                let length = match slaves[index].write(slave_write_buffer.as_slices().0) {
                    Ok(length) => length,
                    Err(ref error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(error) => {
                        return Err(error).prepend_err(&format!(
                            "Failed to write {} bytes to slave #{}.",
                            slave_write_buffer.as_slices().0.len(),
                            index
                        ));
                    }
                };
                self.is_dirty = true;
                slave_write_buffer.drain(..length);
            }
        }

        Ok(())
    }

    pub fn scatter(&mut self) {
        // Eearly return in case that master_read_buffer is empty.
        if self.master_read_buffer.is_empty() {
            return;
        }

        // Build slave write capacity queue.
        self.slave_write_capacity_queue.clear();
        for (index, slave_write_buffer) in self.slave_write_buffers.iter_mut().enumerate() {
            self.slave_write_capacity_queue.push((
                slave_write_buffer.capacity() - slave_write_buffer.len(),
                index,
            ));
        }

        // Scatter master read buffer to slave write buffers.
        while let Some((capacity, index)) = self.slave_write_capacity_queue.pop() {
            // Stop scattering if master read buffer is empty or no capacity in slave write buffers.
            if self.master_read_buffer.is_empty() || capacity < Self::PACKET_SIZE {
                break;
            }

            // Try later if master read buffer is not enough to fill a packet.
            if self.is_dirty
                && self.master_read_buffer.len() < Self::PACKET_SIZE - Self::HEADER_SIZE
            {
                break;
            }

            // Borrow the slave buffer.
            let slave_write_buffer = &mut self.slave_write_buffers[index];
            assert!(
                capacity == slave_write_buffer.capacity() - slave_write_buffer.len(),
                "Multiplexer {}: Unexpected capacity (expected: {}, actual: {}).",
                self.name,
                capacity,
                slave_write_buffer.capacity() - slave_write_buffer.len()
            );

            // Determine the length of a chunk to move.
            let length = std::cmp::min(
                std::cmp::min(Self::PACKET_SIZE, capacity) - Self::HEADER_SIZE,
                self.master_read_buffer.len(),
            );
            assert!(
                0 < length && length <= Self::PACKET_SIZE - Self::HEADER_SIZE,
                "Multiplexer {}: Unexpected length to scatter (actual: {}, expected: 0 < _ <= {}).",
                self.name,
                length,
                Self::PACKET_SIZE - Self::HEADER_SIZE
            );

            self.is_dirty = true;
            if self.scatter_index == 0 {
                info!(
                    "Multiplexer {}: Master's first packet ({} bytes) is arrived.",
                    self.name,
                    length
                );
            }

            // Write the header.
            slave_write_buffer.push_back_usize(self.scatter_index, Self::INDEX_HEADER_SIZE);
            self.scatter_index += 1;
            slave_write_buffer.push_back_usize(length, Self::LENGTH_HEADER_SIZE);

            // Write the contents.
            self.master_read_buffer.drain(..length).for_each(|byte| {
                slave_write_buffer.push_back(byte);
            });

            // Return the slave into the queue again.
            self.slave_write_capacity_queue.push((
                slave_write_buffer.capacity() - slave_write_buffer.len(),
                index,
            ));
            assert!(
                slave_write_buffer.capacity() < Self::SLAVE_BUFFER_SIZE * 2,
                "Multiplexer {}: slave_write_buffer's capacity is too big \
                 (actual: {}, expected: _ < {}).",
                self.name,
                slave_write_buffer.capacity(),
                Self::SLAVE_BUFFER_SIZE * 2
            );
        }
    }

    pub fn gather(&mut self) {
        // For each slave, move chunks which are targetted in the master write buffer.
        for slave_read_buffer in self.slave_read_buffers.iter_mut() {
            while slave_read_buffer.len() >= Self::HEADER_SIZE {
                let length =
                    slave_read_buffer.peek_usize(Self::INDEX_HEADER_SIZE, Self::LENGTH_HEADER_SIZE);
                // Skip if the first chunk is incomplete.
                if slave_read_buffer.len() < length + Self::HEADER_SIZE {
                    break;
                }

                let index = slave_read_buffer
                    .peek_usize(0, Self::INDEX_HEADER_SIZE)
                    .wrapping_sub(self.gather_index)
                    & ((1 << (Self::INDEX_HEADER_SIZE * 8)) - 1);
                // Skip if the chunk is not targetted in the master write buffer.
                if self.master_write_buffers.len() <= index {
                    break;
                }

                // Point to a buffer to write to.
                let master_write_buffer = &mut self.master_write_buffers[index];
                if master_write_buffer.0 {
                    error!(
                        "Multiplexer {}: Write buffer #{} duplicates.",
                        self.name,
                        self.gather_index + index
                    );
                }

                self.is_dirty = true;

                // Populate the write buffer.
                master_write_buffer.0 = true;
                slave_read_buffer
                    .drain(..Self::HEADER_SIZE + length)
                    .skip(Self::HEADER_SIZE)
                    .for_each(|byte| {
                        master_write_buffer.1.push(byte);
                    });
            }
        }
    }

    pub(crate) fn multiplex_once<
        Stream: std::marker::Sized + std::io::Read + std::io::Write + mio::Evented,
    >(
        &mut self,
        master: &mut Stream,
        slaves: &mut Vec<Stream>,
    ) -> std::io::Result<bool> {
        self.is_dirty = false;
        self.write_master(master)?;
        self.write_slaves(slaves)?;
        self.read_master(master)?;
        self.read_slaves(slaves)?;
        self.gather();
        self.scatter();
        Ok(self.is_dirty)
    }

    pub(crate) fn multiplex_while_dirty<
        Stream: std::marker::Sized + std::io::Read + std::io::Write + mio::Evented,
    >(
        &mut self,
        master: &mut Stream,
        slaves: &mut Vec<Stream>,
    ) -> std::io::Result<()> {
        while self.multiplex_once(master, slaves)? {}
        self.stats.flush += 1;
        Ok(())
    }

    fn display_stats(&mut self) {
        self.stats.time = std::time::Instant::now();
        let interval = self.stats.time - self.last_stats.time;
        if interval < std::time::Duration::from_millis(100) {
            return;
        }
        let interval = interval.as_secs() as f64 + interval.subsec_nanos() as f64 * 1.0e-9;
        let bytes_to_mbps = 8.0 / interval / 1024.0 / 1024.0;
        info!(
            "Multiplexer {}: Stats ({:.0} ms): \
             flush = {}; \
             master_read = {} bytes ({:.2} Mbps), {} packets; \
             master_write = {} bytes ({:.2} Mbps), {} packets.",
            self.name,
            interval * 1000.0,
            self.stats.flush,
            self.stats.master_read,
            (self.stats.master_read - self.last_stats.master_read) as f64 * bytes_to_mbps,
            self.stats.master_read_packets,
            self.stats.master_write,
            (self.stats.master_write - self.last_stats.master_write) as f64 * bytes_to_mbps,
            self.stats.master_write_packets,
        );
        self.last_stats = self.stats.clone();
    }

    pub fn multiplex<Stream: std::marker::Sized + std::io::Read + std::io::Write + mio::Evented>(
        &mut self,
        master: &mut Stream,
        slaves: &mut Vec<Stream>,
    ) -> std::io::Result<()> {
        info!(
            "Multiplexer {}: Started a multiplexer with {} slaves.",
            self.name,
            slaves.len()
        );
        let poll = mio::Poll::new().prepend_err("Failed to Poll::new().")?;
        poll.register(
            master,
            mio::Token(0),
            mio::Ready::readable() | mio::Ready::writable(),
            mio::PollOpt::edge(),
        ).prepend_err("Failed to register master to poll.")?;
        for slave in slaves.iter() {
            poll.register(
                slave,
                mio::Token(0),
                mio::Ready::readable() | mio::Ready::writable(),
                mio::PollOpt::edge(),
            ).prepend_err("Failed to register a slave to poll.")?;
        }
        while self.master_connectivity && self.num_of_alive_slaves > 0 {
            if !self.is_dirty {
                // Wait for a new event.
                let polling_start_time = std::time::Instant::now();
                poll.poll(
                    &mut mio::Events::with_capacity(1),
                    Some(std::time::Duration::from_secs(5)),
                ).prepend_err("Failed to poll.")?;

                // Wiat for a short while to prevent packets from being fragmented.
                let polling_time = polling_start_time.elapsed();
                if polling_time < std::time::Duration::from_millis(5) {
                    std::thread::sleep(std::time::Duration::from_millis(5) - polling_time);
                }

                if self.stats.time.elapsed() > std::time::Duration::from_secs(10) {
                    self.display_stats();
                }
            }

            self.multiplex_while_dirty(master, slaves)?;
        }
        info!("Multiplexer {}: Multiplexer is closed.", self.name);
        self.display_stats();

        Ok(())
    }
}

struct MaxTcp {
    config: MaxTcpConfig,
}

#[derive(Clone)]
struct MaxTcpConfig {
    remote_address: std::net::SocketAddr,
    bind_address: std::net::SocketAddr,
    server_mode: bool,
    socket_buffer_size: usize,
    num_of_slaves: usize,
}

impl MaxTcpConfig {
    fn configure_stream(&self, stream: &mio::net::TcpStream) -> std::io::Result<()> {
        stream
            .set_keepalive(Some(std::time::Duration::from_secs(10)))
            .prepend_err("Failed to set_keepalive.")?;
        stream
            .set_nodelay(true)
            .prepend_err("Failed to set_nodelay.")?;
        stream
            .set_send_buffer_size(self.socket_buffer_size)
            .prepend_err("Failed to set_send_buffer_size.")?;
        stream
            .set_recv_buffer_size(self.socket_buffer_size)
            .prepend_err("Failed to set_recv_buffer_size.")?;
        Ok(())
    }
}

struct ServerSocket {
    stream: mio::net::TcpStream,
    remote_address: std::net::SocketAddr,
    buffer: VecDeque<u8>,
}

impl MaxTcp {
    const HEADER_SIZE: usize = 16;

    fn with_args(args: &Vec<String>) -> std::result::Result<Self, String> {
        let mut opts = getopts::Options::new();
        opts.optflag("h", "help", "Print this help.");
        opts.optflag("s", "server", "Enable server mode.");
        opts.optopt(
            "l",
            "listen",
            "Address to listen. (default: 0.0.0.0:2200)",
            "ADDRESS:PORT",
        );
        opts.optopt(
            "c",
            "connections",
            "# of TCP connections per connection.",
            "NUMBER",
        );
        opts.optopt(
            "b",
            "socket_buffer_size",
            "Buffer size for socket buffers.",
            "SIZE",
        );

        let matches = match opts.parse(&args[1..]) {
            Ok(m) => m,
            Err(f) => return Err(format!("Invalid argument: {}", f.to_string())),
        };
        if matches.opt_present("h") {
            print!(
                "{}",
                opts.usage(&format!(
                    "Usage: {} [options] REMOTE_ADDRESS:PORT (BIND_ADDRESS:PORT)",
                    args[0]
                ))
            );
            std::process::exit(0);
        }

        let (remote_address, bind_address): (&str, &str) = match matches.free.len() {
            1 => (&matches.free[0], "0.0.0.0:2200"),
            2 => (&matches.free[0], &matches.free[1]),
            0 => return Err("Missing arguments.".into()),
            num_of_arguments => {
                return Err(format!(
                    "One or two arguments must be given, but {} arguments are found.",
                    num_of_arguments
                ))
            }
        };
        let remote_address: std::net::SocketAddr = match remote_address.parse() {
            Ok(address) => address,
            Err(error) => {
                return Err(format!(
                    "Failed to parse a remote address `{}`: {}",
                    remote_address,
                    error
                ))
            }
        };
        let bind_address: std::net::SocketAddr = match bind_address.parse() {
            Ok(address) => address,
            Err(error) => {
                return Err(format!(
                    "Failed to parse a remote address `{}`: {}",
                    remote_address,
                    error
                ))
            }
        };

        Ok(Self {
            config: MaxTcpConfig {
                remote_address: remote_address,
                bind_address: bind_address,
                server_mode: matches.opt_present("s"),
                socket_buffer_size: matches
                    .opt_str("socket_buffer_size")
                    .unwrap_or("4096".into())
                    .parse()
                    .map_err(|error| {
                        format!("Failed to parse --socket_buffer_size: {}", error)
                    })?,
                num_of_slaves: matches
                    .opt_str("connections")
                    .unwrap_or("16".into())
                    .parse()
                    .map_err(|error| format!("Failed to parse --connections: {}", error))?,
            },
        })
    }

    fn server_thread(
        config: &MaxTcpConfig,
        multiplexer_name: &str,
        slaves: Vec<mio::net::TcpStream>,
        slave_address: std::net::SocketAddr,
    ) -> std::io::Result<()> {
        let master_address = config.remote_address;
        info!("Starting connection from: {}", slave_address);
        let mut master = mio::net::TcpStream::connect(&master_address)
            .prepend_err(&format!("Failed to connect to {}.", &master_address))?;
        {
            let poll = mio::Poll::new().prepend_err("Failed to Poll::new().")?;
            let master = master.try_clone().prepend_err("Failed to clone master.")?;
            poll.register(
                &master,
                mio::Token(0),
                mio::Ready::writable(),
                mio::PollOpt::edge(),
            ).prepend_err("Failed to register a master socket to connect.")?;
            poll.poll(&mut mio::Events::with_capacity(1), None)
                .prepend_err("Failed to poll a master socket.")?;
        }
        let mut slaves = slaves;
        let mut multiplexer = Multiplexer::new(multiplexer_name, slaves.len());
        multiplexer.multiplex(&mut master, &mut slaves)
    }

    fn server(config: &MaxTcpConfig, listener: mio::net::TcpListener) -> std::io::Result<()> {
        info!("Starting server targeting to {}.", config.remote_address);
        let mut next_token = mio::Token(1);
        let mut pending_sockets = std::collections::HashMap::<mio::Token, ServerSocket>::new();
        let mut ready_sockets = std::collections::HashMap::<[u8; 16], Vec<ServerSocket>>::new();
        loop {
            let poll = mio::Poll::new().prepend_err("Failed to Poll::new().")?;
            let mut polling_sockets = Vec::new();
            let listener = listener
                .try_clone()
                .prepend_err("Failed to clone a master socket.")?;
            poll.register(
                &listener,
                mio::Token(0),
                mio::Ready::readable(),
                mio::PollOpt::edge(),
            ).prepend_err("Failed to register to poll a master socket.")?;
            for (token, socket) in &pending_sockets {
                let socket = socket
                    .stream
                    .try_clone()
                    .prepend_err("Failed to clone a slave socket.")?;
                poll.register(
                    &socket,
                    *token,
                    mio::Ready::readable(),
                    mio::PollOpt::edge(),
                ).prepend_err("Failed to register to poll a slave candidate.")?;
                polling_sockets.push(socket);
            }
            let mut events = mio::Events::with_capacity(1024);
            poll.poll(&mut events, None)
                .prepend_err("Failed to poll a lister and pending sockets.")?;
            for event in &events {
                match event.token() {
                    mio::Token(0) => {
                        let (stream, remote_address) =
                            listener.accept().prepend_err("Failed to accept.")?;
                        config
                            .configure_stream(&stream)
                            .prepend_err("Failed to configure a slave socket.")?;
                        pending_sockets.insert(
                            next_token,
                            ServerSocket {
                                stream: stream,
                                remote_address: remote_address,
                                buffer: VecDeque::new(),
                            },
                        );
                        next_token = mio::Token(next_token.0 + 1);
                    }
                    token => {
                        {
                            let socket = match pending_sockets.get_mut(&token) {
                                Some(socket) => socket,
                                None => unreachable!(),
                            };
                            let mut buffer = [0; Self::HEADER_SIZE];
                            let length = socket
                                .stream
                                .read(&mut buffer[0..(Self::HEADER_SIZE - socket.buffer.len())])
                                .prepend_err("Failed to read a header from a pending socket.")?;
                            buffer[..length]
                                .iter()
                                .for_each(|byte| socket.buffer.push_back(*byte));
                            assert!(
                                socket.buffer.len() <= Self::HEADER_SIZE,
                                "Read too many data (actual: {}, expected: _ <= {}).",
                                socket.buffer.len(),
                                Self::HEADER_SIZE
                            );
                            if socket.buffer.len() < Self::HEADER_SIZE {
                                continue;
                            }
                        }
                        let key = if let Some(socket) = pending_sockets.remove(&token) {
                            let mut key = [0; Self::HEADER_SIZE];
                            for position in 0..Self::HEADER_SIZE {
                                key[position] = socket.buffer[position];
                            }
                            let value = ready_sockets.entry(key).or_insert(Vec::new());
                            value.push(socket);
                            if value.len() != key[0] as usize {
                                continue;
                            }
                            key
                        } else {
                            unreachable!();
                        };
                        if let Some(sockets) = ready_sockets.remove(&key) {
                            let config = config.clone();
                            let mut slaves = Vec::new();
                            let socket_address = sockets[0].remote_address;
                            for socket in sockets {
                                slaves.push(socket.stream);
                            }
                            let multiplexer_name =
                                format!("#{:<02x}{:<02x}{:<02x}[S]", key[1], key[2], key[3]);
                            let _ = std::thread::spawn(move || {
                                match Self::server_thread(
                                    &config,
                                    &multiplexer_name,
                                    slaves,
                                    socket_address,
                                ) {
                                    Ok(_) => info!("Connection closed successfully."),
                                    Err(error) => info!("Connection closed: {}", error),
                                }
                            });
                        }
                    }
                };
            }
        }
    }

    fn client_thread(
        config: &MaxTcpConfig,
        stream: mio::net::TcpStream,
        socket_addr: std::net::SocketAddr,
    ) -> std::io::Result<()> {
        info!("Starting a client connection from: {}", socket_addr);
        let mut stream = stream;
        config
            .configure_stream(&stream)
            .prepend_err("Failed to configure a master socket.")?;
        let mut sockets = Vec::new();
        let num_of_slaves = config.num_of_slaves;
        let mut header = [0; Self::HEADER_SIZE];
        rand::thread_rng().fill_bytes(&mut header);
        header[0] = num_of_slaves as u8;
        let multiplexer_name =
            format!("#{:<02x}{:<02x}{:<02x}[C]", header[1], header[2], header[3]);

        for slave_id in 0..num_of_slaves {
            let socket =
                mio::net::TcpStream::connect(&config.remote_address).prepend_err(&format!(
                    "Failed to connect slave #{} of {} slaves.",
                    slave_id,
                    num_of_slaves
                ))?;
            config
                .configure_stream(&socket)
                .prepend_err("Failed to configure a slave socket.")?;
            sockets.push(socket);
        }
        for (slave_id, socket) in sockets.iter_mut().enumerate() {
            let poll = mio::Poll::new().prepend_err("Failed to Poll::new().")?;
            let polling_socket = socket
                .try_clone()
                .prepend_err("Failed to clone a slave socket.")?;
            poll.register(
                &polling_socket,
                mio::Token(0),
                mio::Ready::writable(),
                mio::PollOpt::edge(),
            ).prepend_err("Failed to register a slave socket to write.")?;
            poll.poll(&mut mio::Events::with_capacity(1), None)
                .prepend_err("Failed to poll to write.")?;
            socket.write(&header).prepend_err(&format!(
                "Failed to write header to slave #{} of {} slaves.",
                slave_id,
                num_of_slaves
            ))?;
        }
        let mut multiplexer = Multiplexer::new(&multiplexer_name, sockets.len());
        multiplexer
            .multiplex(&mut stream, &mut sockets)
            .prepend_err("Multiplexer for client failed.")
    }

    fn client(config: &MaxTcpConfig, listener: mio::net::TcpListener) -> std::io::Result<()> {
        let poll = mio::Poll::new().prepend_err("Failed to Poll::new().")?;
        poll.register(
            &listener,
            mio::Token(0),
            mio::Ready::readable(),
            mio::PollOpt::edge(),
        ).prepend_err("Failed to register listener to poll.")?;
        loop {
            poll.poll(&mut mio::Events::with_capacity(1), None)
                .prepend_err("Failed to poll to accept.")?;
            let connection = listener
                .accept()
                .prepend_err("Failed to accept a new connection.")?;
            info!("Accepting a new connection from {}...", connection.1);
            let config = config.clone();
            std::thread::spawn(move || {
                match Self::client_thread(&config, connection.0, connection.1) {
                    Ok(_) => info!("Connection closed successfully."),
                    Err(error) => info!("Connection closed by error:\n- {}", error),
                }
            });
        }
    }

    fn run(&self) -> std::io::Result<()> {
        info!("Listening address `{}`", &self.config.bind_address);
        let listener = match mio::net::TcpListener::bind(&self.config.bind_address) {
            Ok(listener) => listener,
            Err(error) => {
                return Err(std::io::Error::new(
                    error.kind(),
                    format!(
                        "Failed to bind address `{}`: {}",
                        &self.config.bind_address,
                        error
                    ),
                ));
            }
        };
        if self.config.server_mode {
            MaxTcp::server(&self.config, listener).prepend_err("MaxTcp::server failed.")
        } else {
            MaxTcp::client(&self.config, listener).prepend_err("MaxTcp::client failed.")
        }
    }
}

fn main() {
    env_logger::init();

    let error = match MaxTcp::with_args(&std::env::args().collect()) {
        Ok(max_tcp) => match max_tcp.run() {
            Err(error) => format!("{}", error),
            Ok(_) => std::process::exit(0),
        },
        Err(error) => format!("{}", error),
    };
    eprintln!("{}", error);
    std::process::exit(1);
}
