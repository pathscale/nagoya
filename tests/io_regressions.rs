use nagoya::block_on;
use nagoya::io::{Error, ErrorKind, Read, Write};

struct InterruptedReader {
    bytes: &'static [u8],
    cursor: usize,
    interrupt: bool,
}

impl Read for InterruptedReader {
    async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Error> {
        if self.interrupt {
            self.interrupt = false;
            return Err(Error::new(ErrorKind::Interrupted));
        }
        let n = buffer.len().min(2).min(self.bytes.len() - self.cursor);
        buffer[..n].copy_from_slice(&self.bytes[self.cursor..self.cursor + n]);
        self.cursor += n;
        self.interrupt = true;
        Ok(n)
    }
}

#[derive(Default)]
struct InterruptedWriter {
    bytes: Vec<u8>,
    interrupt: bool,
}

impl Write for InterruptedWriter {
    async fn write(&mut self, buffer: &[u8]) -> Result<usize, Error> {
        if self.interrupt {
            self.interrupt = false;
            return Err(Error::new(ErrorKind::Interrupted));
        }
        let n = buffer.len().min(2);
        self.bytes.extend_from_slice(&buffer[..n]);
        self.interrupt = true;
        Ok(n)
    }

    async fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

#[test]
fn read_exact_keeps_progress_across_interruptions() {
    let mut reader = InterruptedReader {
        bytes: b"abcdef",
        cursor: 0,
        interrupt: true,
    };
    let mut bytes = [0; 6];
    block_on(reader.read_exact(&mut bytes)).unwrap();
    assert_eq!(&bytes, b"abcdef");
    assert_eq!(reader.cursor, 6);
}

#[test]
fn read_to_end_keeps_the_prefix_and_drops_scratch_space_on_interruption() {
    let mut reader = InterruptedReader {
        bytes: b"abcdef",
        cursor: 0,
        interrupt: true,
    };
    let mut bytes = b"prefix:".to_vec();
    assert_eq!(block_on(reader.read_to_end(&mut bytes)).unwrap(), 6);
    assert_eq!(&bytes, b"prefix:abcdef");
}

#[test]
fn write_all_neither_repeats_nor_skips_bytes_after_interruption() {
    let mut writer = InterruptedWriter {
        interrupt: true,
        ..Default::default()
    };
    block_on(writer.write_all(b"abcdef")).unwrap();
    assert_eq!(&writer.bytes, b"abcdef");
}

#[test]
fn real_eof_is_still_an_error_for_read_exact() {
    let mut reader = InterruptedReader {
        bytes: b"ab",
        cursor: 0,
        interrupt: false,
    };
    let mut bytes = [0; 3];
    assert_eq!(
        block_on(reader.read_exact(&mut bytes)).unwrap_err().kind(),
        ErrorKind::UnexpectedEof,
    );
    assert_eq!(&bytes[..2], b"ab");
}
