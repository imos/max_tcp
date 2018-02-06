#![cfg(test)]

use super::*;
use rand::Rng;

#[test]
fn test_init() {
    let _ = Multiplexer::new("", 0);
    let _ = Multiplexer::new("", 1);
    let _ = Multiplexer::new("", 100);
}

trait ToString {
    fn to_string(&self) -> String;
}

impl ToString for VecDeque<u8> {
    fn to_string(&self) -> String {
        String::from_utf8(self.iter().map(|byte| *byte).collect()).unwrap()
    }
}

trait VecTestUtil {
    fn push_back_str(&mut self, data: &str);
}

impl VecTestUtil for VecDeque<u8> {
    fn push_back_str(&mut self, data: &str) {
        data.bytes().for_each(|byte| self.push_back(byte));
    }
}

impl VecTestUtil for Vec<u8> {
    fn push_back_str(&mut self, data: &str) {
        data.bytes().for_each(|byte| self.push(byte));
    }
}

#[derive(Copy, Clone, PartialEq, PartialOrd)]
struct Clock(usize);

struct FakeClock(std::sync::Mutex<Clock>);

impl FakeClock {
    fn new() -> FakeClock {
        FakeClock(std::sync::Mutex::new(Clock(0)))
    }

    fn set(&self, clock: Clock) {
        *self.0.lock().unwrap() = clock;
    }

    fn get(&self) -> Clock {
        *self.0.lock().unwrap()
    }
}

struct FakeStream<'a> {
    fake_clock: &'a FakeClock,
    is_dead: bool,
    reads: VecDeque<(Clock, std::io::Result<VecDeque<u8>>)>,
    writes: VecDeque<u8>,
}

impl<'a> FakeStream<'a> {
    pub fn new(fake_clock: &'a FakeClock) -> FakeStream<'a> {
        FakeStream {
            fake_clock: fake_clock,
            is_dead: false,
            reads: VecDeque::new(),
            writes: VecDeque::new(),
        }
    }

    pub fn push_str_to_read(&mut self, clock: Clock, data: &str) {
        self.reads
            .push_back((clock, Ok(data.bytes().collect::<VecDeque<u8>>())));
    }

    pub fn mark_as_dead(&mut self) {
        self.is_dead = true;
    }
}

impl<'a> std::io::Read for FakeStream<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.reads.len() == 0 && self.is_dead {
            return Ok(0);
        } else if self.reads.len() == 0 || self.fake_clock.get() < self.reads[0].0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "Waiting for data.",
            ));
        }
        let (read_clock, read_buffer) = match self.reads.pop_front() {
            Some(read) => read,
            None => return Ok(0),
        };
        let mut read_buffer = match read_buffer {
            Ok(read_buffer) => read_buffer,
            Err(error) => return Err(error),
        };
        let mut length = 0;
        while length < buf.len() {
            buf[length] = match read_buffer.pop_front() {
                Some(byte) => byte,
                None => break,
            };
            length += 1;
        }
        if !read_buffer.is_empty() {
            self.reads.push_front((read_clock, Ok(read_buffer)));
        }
        Ok(length)
    }
}

impl<'a> std::io::Write for FakeStream<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.is_dead {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Connection is dead.",
            ));
        }
        for byte in buf {
            self.writes.push_back(*byte);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> mio::Evented for FakeStream<'a> {
    fn register(
        &self,
        _: &mio::Poll,
        _: mio::Token,
        _: mio::Ready,
        _: mio::PollOpt,
    ) -> std::io::Result<()> {
        Ok(())
    }

    fn reregister(
        &self,
        _: &mio::Poll,
        _: mio::Token,
        _: mio::Ready,
        _: mio::PollOpt,
    ) -> std::io::Result<()> {
        Ok(())
    }

    fn deregister(&self, _: &mio::Poll) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn test_master_read() {
    let mut multiplexer = Multiplexer::new("", 0);
    let fake_clock = FakeClock::new();
    let mut fake_master = FakeStream::new(&fake_clock);

    fake_master.push_str_to_read(Clock(0), "TEST1");
    fake_master.push_str_to_read(Clock(1), "TEST2");

    multiplexer.is_dirty = false;
    multiplexer.read_master(&mut fake_master).unwrap();
    assert_eq!(multiplexer.master_read_buffer.to_string(), "TEST1");
    assert!(multiplexer.is_dirty, "read should mark it as dirty.");

    fake_clock.set(Clock(1));
    multiplexer.is_dirty = false;
    multiplexer.read_master(&mut fake_master).unwrap();
    assert_eq!(multiplexer.master_read_buffer.to_string(), "TEST1TEST2");
    assert!(multiplexer.is_dirty, "read should mark it as dirty.");

    fake_clock.set(Clock(2));
    multiplexer.is_dirty = false;
    multiplexer.read_master(&mut fake_master).unwrap();
    assert!(!multiplexer.is_dirty, "No read should happen.");

    assert!(multiplexer.master_connectivity);
}

#[test]
fn test_master_read_should_not_recieve_excess_data() {
    let mut multiplexer = Multiplexer::new("", 0);
    let fake_clock = FakeClock::new();
    let mut fake_master = FakeStream::new(&fake_clock);

    for _ in 0..Multiplexer::MASTER_BUFFER_SIZE / 4 {
        fake_master.push_str_to_read(Clock(0), "0123456789abcdef0123456789abcdef");
    }

    multiplexer.read_master(&mut fake_master).unwrap();
    assert!(
        multiplexer.master_read_buffer.len() >= multiplexer.master_read_buffer.capacity() - 4,
        "master_read_buffer must be full (actual: {}, expected: _ >= {}).",
        multiplexer.master_read_buffer.len(),
        multiplexer.master_read_buffer.capacity() - 4
    );

    multiplexer.master_read_buffer.clear();
    multiplexer.read_master(&mut fake_master).unwrap();
    assert!(
        multiplexer.master_read_buffer.len() >= multiplexer.master_read_buffer.capacity() - 4,
        "master_read_buffer must be full (actual: {}, expected: _ >= {}).",
        multiplexer.master_read_buffer.len(),
        multiplexer.master_read_buffer.capacity() - 4
    );
}

#[test]
fn test_master_read_can_detect_eof() {
    let mut multiplexer = Multiplexer::new("", 0);
    let fake_clock = FakeClock::new();
    let mut fake_master = FakeStream::new(&fake_clock);

    fake_master.push_str_to_read(Clock(0), "TEST1");
    fake_master.mark_as_dead();

    multiplexer.is_dirty = false;
    assert!(multiplexer.master_connectivity);
    multiplexer.read_master(&mut fake_master).unwrap();
    assert_eq!(multiplexer.master_read_buffer.to_string(), "TEST1");
    assert!(multiplexer.is_dirty, "read should mark it as dirty.");
    assert!(!multiplexer.master_connectivity);
}

#[test]
fn test_master_write() {
    let mut multiplexer = Multiplexer::new("", 0);
    let fake_clock = FakeClock::new();
    let mut fake_master = FakeStream::new(&fake_clock);

    multiplexer.master_write_buffers[0].0 = true;
    multiplexer.master_write_buffers[0].1.push_back_str("TEST1");
    multiplexer.master_write_buffers[2].0 = true;
    multiplexer.master_write_buffers[2].1.push_back_str("TEST3");

    multiplexer.is_dirty = false;
    multiplexer.write_master(&mut fake_master).unwrap();
    assert_eq!(fake_master.writes.to_string(), "TEST1");
    assert!(multiplexer.is_dirty, "write should mark it as dirty.");
    assert_eq!(multiplexer.gather_index, 1);

    multiplexer.master_write_buffers[0].0 = true;
    multiplexer.master_write_buffers[0].1.push_back_str("TEST2");

    multiplexer.is_dirty = false;
    multiplexer.write_master(&mut fake_master).unwrap();
    assert_eq!(fake_master.writes.to_string(), "TEST1TEST2TEST3");
    assert!(multiplexer.is_dirty, "write should mark it as dirty.");
    assert_eq!(multiplexer.gather_index, 3);

    multiplexer.is_dirty = false;
    multiplexer.write_master(&mut fake_master).unwrap();
    assert!(!multiplexer.is_dirty, "No write should happen.");
}

#[test]
fn test_slave_read() {
    let fake_clock = FakeClock::new();
    let mut fake_slaves = Vec::new();
    for _ in 0..3 {
        fake_slaves.push(FakeStream::new(&fake_clock));
    }
    let mut multiplexer = Multiplexer::new("", fake_slaves.len());

    fake_slaves[0].push_str_to_read(Clock(1), "TEST1");
    fake_slaves[0].mark_as_dead();
    fake_slaves[1].push_str_to_read(Clock(0), "TEST2");
    fake_slaves[2].push_str_to_read(Clock(1), "TEST3");
    fake_slaves[2].push_str_to_read(Clock(2), "TEST4");
    fake_slaves[2].mark_as_dead();

    multiplexer.is_dirty = false;
    multiplexer.read_slaves(&mut fake_slaves).unwrap();
    assert_eq!(multiplexer.slave_read_buffers[0].to_string(), "");
    assert_eq!(multiplexer.slave_read_buffers[1].to_string(), "TEST2");
    assert_eq!(multiplexer.slave_read_buffers[2].to_string(), "");
    assert_eq!(multiplexer.num_of_alive_slaves, 3);
    assert!(multiplexer.is_dirty, "read should mark it as dirty.");

    fake_clock.set(Clock(1));
    multiplexer.is_dirty = false;
    multiplexer.read_slaves(&mut fake_slaves).unwrap();
    assert_eq!(multiplexer.slave_read_buffers[0].to_string(), "TEST1");
    assert_eq!(multiplexer.slave_read_buffers[1].to_string(), "TEST2");
    assert_eq!(multiplexer.slave_read_buffers[2].to_string(), "TEST3");
    // Slave #0 should close.
    assert_eq!(multiplexer.num_of_alive_slaves, 2);
    assert!(multiplexer.is_dirty, "read should mark it as dirty.");

    fake_clock.set(Clock(2));
    multiplexer.is_dirty = false;
    multiplexer.read_slaves(&mut fake_slaves).unwrap();
    assert_eq!(multiplexer.slave_read_buffers[0].to_string(), "TEST1");
    assert_eq!(multiplexer.slave_read_buffers[1].to_string(), "TEST2");
    assert_eq!(multiplexer.slave_read_buffers[2].to_string(), "TEST3TEST4");
    // Slave #2 should close.
    assert_eq!(multiplexer.num_of_alive_slaves, 1);
    assert!(multiplexer.is_dirty, "read should mark it as dirty.");

    fake_clock.set(Clock(3));
    multiplexer.is_dirty = false;
    multiplexer.read_slaves(&mut fake_slaves).unwrap();
    assert_eq!(multiplexer.slave_read_buffers[0].to_string(), "TEST1");
    assert_eq!(multiplexer.slave_read_buffers[1].to_string(), "TEST2");
    assert_eq!(multiplexer.slave_read_buffers[2].to_string(), "TEST3TEST4");
    assert_eq!(multiplexer.num_of_alive_slaves, 1);
    assert!(!multiplexer.is_dirty, "No read should happen.");
}

#[test]
fn test_slave_write() {
    let fake_clock = FakeClock::new();
    let mut fake_slaves = Vec::new();
    for _ in 0..3 {
        fake_slaves.push(FakeStream::new(&fake_clock));
    }
    let mut multiplexer = Multiplexer::new("", fake_slaves.len());

    multiplexer.slave_write_buffers[1].push_back_str("TEST1");

    multiplexer.is_dirty = false;
    multiplexer.write_slaves(&mut fake_slaves).unwrap();
    assert_eq!(fake_slaves[0].writes.to_string(), "");
    assert_eq!(fake_slaves[1].writes.to_string(), "TEST1");
    assert_eq!(fake_slaves[2].writes.to_string(), "");
    assert!(multiplexer.is_dirty, "write should mark it as dirty.");

    multiplexer.slave_write_buffers[0].push_back_str("TEST2");
    multiplexer.slave_write_buffers[1].push_back_str("TEST3");
    multiplexer.slave_write_buffers[2].push_back_str("TEST4");

    multiplexer.is_dirty = false;
    multiplexer.write_slaves(&mut fake_slaves).unwrap();
    assert_eq!(fake_slaves[0].writes.to_string(), "TEST2");
    assert_eq!(fake_slaves[1].writes.to_string(), "TEST1TEST3");
    assert_eq!(fake_slaves[2].writes.to_string(), "TEST4");
    assert!(multiplexer.is_dirty, "write should mark it as dirty.");

    multiplexer.is_dirty = false;
    multiplexer.write_slaves(&mut fake_slaves).unwrap();
    assert_eq!(fake_slaves[0].writes.to_string(), "TEST2");
    assert_eq!(fake_slaves[1].writes.to_string(), "TEST1TEST3");
    assert_eq!(fake_slaves[2].writes.to_string(), "TEST4");
    assert!(!multiplexer.is_dirty, "No write should happen.");
}

#[test]
fn test_scatter() {
    let mut multiplexer = Multiplexer::new("", 2);

    {
        multiplexer.master_read_buffer.push_back_str("TESTTEST1");

        multiplexer.is_dirty = false;
        multiplexer.scatter();
        assert_eq!(multiplexer.slave_write_buffers[0].to_string(), "");
        assert_eq!(
            multiplexer.slave_write_buffers[1].to_string(),
            "\x00\x00\x00\x00\x09\x00TESTTEST1"
        );
        assert!(multiplexer.is_dirty, "scatter should mark it as dirty.");
    }
    {
        multiplexer.master_read_buffer.push_back_str("TEST2");

        multiplexer.is_dirty = false;
        multiplexer.scatter();
        assert_eq!(
            multiplexer.slave_write_buffers[0].to_string(),
            "\x01\x00\x00\x00\x05\x00TEST2"
        );
        assert_eq!(
            multiplexer.slave_write_buffers[1].to_string(),
            "\x00\x00\x00\x00\x09\x00TESTTEST1"
        );
        assert!(multiplexer.is_dirty, "scatter should mark it as dirty.");
    }
    {
        multiplexer.master_read_buffer.push_back_str("TEST3");

        multiplexer.is_dirty = false;
        multiplexer.scatter();
        assert_eq!(
            multiplexer.slave_write_buffers[0].to_string(),
            "\x01\x00\x00\x00\x05\x00TEST2\x02\x00\x00\x00\x05\x00TEST3"
        );
        assert_eq!(
            multiplexer.slave_write_buffers[1].to_string(),
            "\x00\x00\x00\x00\x09\x00TESTTEST1"
        );
        assert!(multiplexer.is_dirty, "scatter should mark it as dirty.");
    }
    {
        multiplexer.is_dirty = false;
        multiplexer.scatter();
        assert!(!multiplexer.is_dirty, "No scatter should happen.");
    }
}

#[test]
fn test_gather() {
    let mut multiplexer = Multiplexer::new("", 2);
    let fake_clock = FakeClock::new();
    let mut fake_master = FakeStream::new(&fake_clock);

    multiplexer.slave_read_buffers[0].push_back_str("\x00\x00\x00\x00\x06\x00TEST1X");
    multiplexer.slave_read_buffers[0].push_back_str("\x03\x00\x00\x00\x09\x00TEST4XXXX");
    // Incomplete chunk cannot be accepted.
    multiplexer.slave_read_buffers[0].push_back_str("\x04\x00\x00\x00\x0A\x00TEST5XXXX");

    // Receive the first chunk only.
    {
        multiplexer.is_dirty = false;
        multiplexer.gather();
        assert!(
            multiplexer.is_dirty,
            "gather should mark the multiplexer as dirty."
        );
        multiplexer.write_master(&mut fake_master).unwrap();
        assert_eq!(fake_master.writes.to_string(), "TEST1X");
    }

    multiplexer.slave_read_buffers[1].push_back_str("\x01\x00\x00\x00\x07\x00TEST2XX");
    multiplexer.slave_read_buffers[1].push_back_str("\x02\x00\x00\x00\x08\x00TEST3XXX");

    // Receive chunks 3 more chunks.
    {
        multiplexer.is_dirty = false;
        multiplexer.gather();
        assert!(
            multiplexer.is_dirty,
            "gather should mark the multiplexer as dirty."
        );
        multiplexer.write_master(&mut fake_master).unwrap();
        assert_eq!(
            fake_master.writes.to_string(),
            "TEST1XTEST2XXTEST3XXXTEST4XXXX"
        );
    }

    // Cannot receive more complete chunks because the next chunk is incomplete.
    {
        multiplexer.is_dirty = false;
        multiplexer.gather();
        assert!(!multiplexer.is_dirty, "gather should not happen.");
        multiplexer.write_master(&mut fake_master).unwrap();
        assert_eq!(
            fake_master.writes.to_string(),
            "TEST1XTEST2XXTEST3XXXTEST4XXXX"
        );
    }

    // Complete the chunk, then it can be received.
    multiplexer.slave_read_buffers[0].push_back_str("X");
    {
        multiplexer.is_dirty = false;
        multiplexer.gather();
        assert!(
            multiplexer.is_dirty,
            "gather should mark the multiplexer as dirty."
        );
        multiplexer.write_master(&mut fake_master).unwrap();
        assert_eq!(
            fake_master.writes.to_string(),
            "TEST1XTEST2XXTEST3XXXTEST4XXXXTEST5XXXXX"
        );
    }
}

#[test]
fn test_integration() {
    use rand::SeedableRng;

    let fake_clock = FakeClock::new();
    let mut fake_slaves = Vec::new();
    for _ in 0..3 {
        fake_slaves.push(FakeStream::new(&fake_clock));
    }
    let mut fake_master = FakeStream::new(&fake_clock);
    let mut multiplexer = Multiplexer::new("", fake_slaves.len());
    let mut send_rng = rand::XorShiftRng::from_seed([1; 4]);
    let mut recv_rng = rand::XorShiftRng::from_seed([1; 4]);
    let mut parameter_rng = rand::XorShiftRng::from_seed([2; 4]);
    let mut num_of_sent_bytes = 0;

    for senario in 0..3 {
        let (master_speed, slave_speed) = match senario {
            0 => (100 * fake_slaves.len(), 100),
            1 => (100 * fake_slaves.len(), 10),
            2 => (0, 1000),
            _ => unreachable!(),
        };
        for _ in 0..1000 {
            let mut data = VecDeque::new();
            if master_speed > 0 {
                for _ in 0..parameter_rng.gen_range(0, master_speed) {
                    data.push_back(send_rng.gen());
                    num_of_sent_bytes += 1;
                }
            }
            fake_master.reads.push_back((fake_clock.get(), Ok(data)));

            multiplexer
                .multiplex_while_dirty(&mut fake_master, &mut fake_slaves)
                .unwrap();

            for fake_slave in fake_slaves.iter_mut() {
                let length = std::cmp::min(
                    fake_slave.writes.len(),
                    parameter_rng.gen_range(0, slave_speed),
                );
                if length > 0 {
                    fake_slave.reads.push_back((
                        fake_clock.get(),
                        Ok(fake_slave.writes.drain(..length).collect()),
                    ));
                }
            }
        }
        println!(
            "Send: {} bytes, Recv: {} bytes",
            num_of_sent_bytes,
            fake_master.writes.len()
        );
    }

    for (index, byte) in fake_master.writes.iter().enumerate() {
        assert_eq!(
            *byte,
            recv_rng.gen::<u8>(),
            "Received unexpected data in #{}.",
            index
        );
    }
    assert_eq!(num_of_sent_bytes, fake_master.writes.len());
}
