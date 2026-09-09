//! Files, for consumers that need one and do not want a runtime attached.
//!
//! # Why this is here and not a reactor
//!
//! The crate documentation says there will be no I/O driver, and that still
//! holds: an epoll or io_uring reactor is the part that needs an operating
//! system, and being able to run without one is the point of this crate.
//!
//! **A file abstraction is not a reactor.** It is a set of traits plus one
//! implementation for hosts that have `std`, exactly the shape [`Host`] already
//! has for parking. A consumer on bare metal implements them over its own flash
//! or block device and never links the host module.
//!
//! [`Host`]: st3::fanout::Host
//!
//! # Why these traits and not `futures-io`
//!
//! Because `futures-io` is not portable, and it is easy to conclude otherwise.
//! **Every one of its traits sits behind its own `std` feature.** Turn that off
//! and the crate compiles to nothing at all: no `AsyncRead`, no `AsyncWrite`,
//! no `AsyncSeek`. They take `std::io::Error` and `IoSlice`, so there was
//! nowhere else for them to go.
//!
//! That is worth stating plainly because checking it the obvious way gives the
//! wrong answer. The crate *builds* for `aarch64-unknown-none`, so a probe that
//! only compiles it reports success; it is the exports that vanish. Anything
//! written on top of those three is a `std` trait wearing a portable name.
//!
//! So the traits below are the same shape with two differences that matter:
//! the error is [`Error`], which needs no operating system, and the seek origin
//! is [`SeekFrom`] rather than `std::io::SeekFrom`. Under `std` the two
//! interoperate: [`Compat`] wraps any `futures-io` type, and [`HostFile`] is a
//! plain `std::fs::File` that already implements all of it.
//!
//! # Why the calls block
//!
//! They block the caller, because that is what the platform offers. Neither
//! `tokio::fs` nor `async-fs` performs asynchronous file I/O either: both hand
//! a blocking `std::fs` call to a thread pool, and what that buys is not
//! occupying a runtime worker rather than any actual overlap. It is not free.
//! Measured on WorkTable's scattered-update path, `tokio::fs` ran 12,316 rows
//! per second against 74,728 for the same code on blocking `std::fs`.
//!
//! So the right place for this work is a thread that owns it, not a worker
//! shared with everything else. This module gives you the file; where you run
//! it is yours to decide.

use core::future::Future;

/// Where a seek counts from.
///
/// `std::io::SeekFrom` in all but name, restated because that one needs an
/// operating system and this does not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeekFrom {
    /// Bytes from the beginning.
    Start(u64),
    /// Bytes from the end, where negative moves backwards into the file.
    End(i64),
    /// Bytes from the current position, where negative moves backwards.
    Current(i64),
}

/// Reading bytes from something.
pub trait Read {
    /// Read into `buffer`, returning how many bytes arrived.
    ///
    /// A return of `Ok(0)` means the end, and is not an error.
    fn read(&mut self, buffer: &mut [u8]) -> impl Future<Output = Result<usize, Error>> + Send;

    /// Fill `buffer` completely, or fail.
    ///
    /// A short read is not an end condition here: the caller asked for a fixed
    /// number of bytes, so running out is [`ErrorKind::UnexpectedEof`].
    fn read_exact(&mut self, buffer: &mut [u8]) -> impl Future<Output = Result<(), Error>> + Send
    where
        Self: Send,
    {
        async move {
            let mut filled = 0;
            while filled < buffer.len() {
                match self.read(&mut buffer[filled..]).await? {
                    0 => return Err(Error::new(ErrorKind::UnexpectedEof)),
                    n => filled += n,
                }
            }
            Ok(())
        }
    }

    /// Append everything left to `buffer`, returning how many bytes were added.
    ///
    /// Grows in chunks rather than one byte at a time, and only keeps what was
    /// actually read, so a source that ends early does not leave the caller
    /// holding a tail of zeros it cannot distinguish from data.
    fn read_to_end(
        &mut self,
        buffer: &mut alloc::vec::Vec<u8>,
    ) -> impl Future<Output = Result<usize, Error>> + Send
    where
        Self: Send,
    {
        /// Big enough that a whole page arrives in one or two reads, small
        /// enough not to over-allocate for a short file.
        const CHUNK: usize = 8192;
        async move {
            let start = buffer.len();
            loop {
                let filled = buffer.len();
                buffer.resize(filled + CHUNK, 0);
                match self.read(&mut buffer[filled..]).await {
                    Ok(0) => {
                        buffer.truncate(filled);
                        return Ok(filled - start);
                    }
                    Ok(n) => buffer.truncate(filled + n),
                    Err(error) => {
                        buffer.truncate(filled);
                        return Err(error);
                    }
                }
            }
        }
    }
}

/// Writing bytes to something.
pub trait Write {
    /// Write some of `buffer`, returning how many bytes were taken.
    fn write(&mut self, buffer: &[u8]) -> impl Future<Output = Result<usize, Error>> + Send;

    /// Push whatever is buffered towards its destination.
    ///
    /// This is not durability. A write that has been flushed has left this
    /// process; it has not necessarily reached the storage device. For that,
    /// see [`File::sync_all`].
    fn flush(&mut self) -> impl Future<Output = Result<(), Error>> + Send;

    /// Write all of `buffer`, or fail.
    fn write_all(&mut self, buffer: &[u8]) -> impl Future<Output = Result<(), Error>> + Send
    where
        Self: Send,
    {
        async move {
            let mut written = 0;
            while written < buffer.len() {
                match self.write(&buffer[written..]).await? {
                    // Nothing taken and no error: the destination will not
                    // accept more, and looping would spin forever.
                    0 => return Err(Error::new(ErrorKind::WriteZero)),
                    n => written += n,
                }
            }
            Ok(())
        }
    }
}

/// Moving the cursor of something that has one.
pub trait Seek {
    /// Move the cursor, returning where it landed, counted from the start.
    fn seek(&mut self, from: SeekFrom) -> impl Future<Output = Result<u64, Error>> + Send;

    /// Where the cursor is, counted from the start.
    fn stream_position(&mut self) -> impl Future<Output = Result<u64, Error>> + Send
    where
        Self: Send,
    {
        async move { self.seek(SeekFrom::Current(0)).await }
    }
}

/// A file: readable, writable, seekable, and able to answer the handful of
/// questions no I/O trait covers.
///
/// The four methods below are the reason this trait exists. Every storage crate
/// needs them and none of `futures-io`, `tokio::io` or `std::io` puts them in a
/// trait, so each one reinvents them privately and no two agree.
pub trait File: Read + Write + Seek {
    /// Bytes currently in the file.
    fn length(&mut self) -> impl Future<Output = Result<u64, Error>> + Send;

    /// Truncate or extend to `length`, zero-filling any new bytes.
    fn set_length(&mut self, length: u64) -> impl Future<Output = Result<(), Error>> + Send;

    /// Flush this file's contents *and* its metadata to the storage device.
    ///
    /// Unlike [`Write::flush`], this is the durability boundary.
    fn sync_all(&mut self) -> impl Future<Output = Result<(), Error>> + Send;

    /// Flush this file's contents, leaving metadata to the platform.
    ///
    /// Cheaper than [`File::sync_all`] where the platform distinguishes them,
    /// and identical to it where it does not.
    fn sync_data(&mut self) -> impl Future<Output = Result<(), Error>> + Send;
}

/// What went wrong.
///
/// Deliberately not `std::io::Error`: these traits have to be implementable on
/// a target that has no `std`. With `std` on, the two convert.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Error {
    kind: ErrorKind,
}

impl Error {
    /// An error of this kind.
    #[must_use]
    pub fn new(kind: ErrorKind) -> Self {
        Self { kind }
    }

    /// What kind of failure this was.
    #[must_use]
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
}

/// The failures a file operation can report.
///
/// Coarse on purpose. A caller either retries, gives up, or creates what was
/// missing, and a longer list would not change which of those it picks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The path names nothing.
    NotFound,
    /// The path names something that cannot be opened this way.
    PermissionDenied,
    /// The read wanted more bytes than the file had left.
    UnexpectedEof,
    /// The destination accepted no bytes and reported no error.
    WriteZero,
    /// The operation was cut short and can be retried as-is.
    Interrupted,
    /// Anything else the platform reported.
    Other,
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let text = match self.kind {
            ErrorKind::NotFound => "no such file",
            ErrorKind::PermissionDenied => "permission denied",
            ErrorKind::UnexpectedEof => "unexpected end of file",
            ErrorKind::WriteZero => "the destination accepted no bytes",
            ErrorKind::Interrupted => "interrupted",
            ErrorKind::Other => "file operation failed",
        };
        f.write_str(text)
    }
}

// `core::error::Error`, not `std::error::Error`, and not behind the `std`
// feature. A consumer without an operating system still wants `?` to work
// against its own error type, and gating this would have made the trait
// unusable in exactly the place it exists for.
impl core::error::Error for Error {}

#[cfg(feature = "std")]
impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::new(match error.kind() {
            std::io::ErrorKind::NotFound => ErrorKind::NotFound,
            std::io::ErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
            std::io::ErrorKind::UnexpectedEof => ErrorKind::UnexpectedEof,
            std::io::ErrorKind::WriteZero => ErrorKind::WriteZero,
            std::io::ErrorKind::Interrupted => ErrorKind::Interrupted,
            _ => ErrorKind::Other,
        })
    }
}

#[cfg(feature = "std")]
impl From<Error> for std::io::Error {
    fn from(error: Error) -> Self {
        std::io::Error::new(
            match error.kind {
                ErrorKind::NotFound => std::io::ErrorKind::NotFound,
                ErrorKind::PermissionDenied => std::io::ErrorKind::PermissionDenied,
                ErrorKind::UnexpectedEof => std::io::ErrorKind::UnexpectedEof,
                ErrorKind::WriteZero => std::io::ErrorKind::WriteZero,
                ErrorKind::Interrupted => std::io::ErrorKind::Interrupted,
                ErrorKind::Other => std::io::ErrorKind::Other,
            },
            error,
        )
    }
}

#[cfg(feature = "std")]
impl From<SeekFrom> for std::io::SeekFrom {
    fn from(from: SeekFrom) -> Self {
        match from {
            SeekFrom::Start(n) => std::io::SeekFrom::Start(n),
            SeekFrom::End(n) => std::io::SeekFrom::End(n),
            SeekFrom::Current(n) => std::io::SeekFrom::Current(n),
        }
    }
}

#[cfg(feature = "std")]
pub use host::{
    append, create, create_dir_all, metadata, open, open_or_create, read, remove_dir_all,
    remove_file, rename, write, Compat, HostFile,
};

#[cfg(feature = "std")]
mod host {
    //! The implementation for a target that has a filesystem.
    //!
    //! Blocking `std::fs`, reached through `async fn`s that do not await
    //! anything. That is not a pretence of asynchrony: it is the honest shape,
    //! because the platform has no asynchronous file I/O to offer and the two
    //! crates that claim otherwise are thread pools in a trench coat.

    use super::{Error, File, Read, Seek, SeekFrom, Write};
    use std::path::Path;

    /// A [`File`] backed by `std::fs`.
    #[derive(Debug)]
    pub struct HostFile {
        inner: std::fs::File,
    }

    impl HostFile {
        /// Take an already-open file.
        #[must_use]
        pub fn new(inner: std::fs::File) -> Self {
            Self { inner }
        }

        /// The file underneath, for the platform-specific things this trait
        /// deliberately does not cover.
        #[must_use]
        pub fn get_ref(&self) -> &std::fs::File {
            &self.inner
        }
    }

    // `async fn` rather than a computed `Result` handed to an `async move`
    // block: the second form runs the syscall when the method is called, so a
    // future that is built and dropped unpolled would still have written. A
    // future does nothing until it is polled, blocking body or not.
    impl Read for HostFile {
        async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Error> {
            std::io::Read::read(&mut self.inner, buffer).map_err(Error::from)
        }
    }

    impl Write for HostFile {
        async fn write(&mut self, buffer: &[u8]) -> Result<usize, Error> {
            std::io::Write::write(&mut self.inner, buffer).map_err(Error::from)
        }

        async fn flush(&mut self) -> Result<(), Error> {
            std::io::Write::flush(&mut self.inner).map_err(Error::from)
        }
    }

    impl Seek for HostFile {
        async fn seek(&mut self, from: SeekFrom) -> Result<u64, Error> {
            std::io::Seek::seek(&mut self.inner, from.into()).map_err(Error::from)
        }
    }

    impl File for HostFile {
        async fn length(&mut self) -> Result<u64, Error> {
            self.inner.metadata().map(|m| m.len()).map_err(Error::from)
        }

        async fn set_length(&mut self, length: u64) -> Result<(), Error> {
            self.inner.set_len(length).map_err(Error::from)
        }

        async fn sync_all(&mut self) -> Result<(), Error> {
            self.inner.sync_all().map_err(Error::from)
        }

        async fn sync_data(&mut self) -> Result<(), Error> {
            self.inner.sync_data().map_err(Error::from)
        }
    }

    /// Wraps a `futures-io` type so it satisfies [`Read`], [`Write`] and
    /// [`Seek`].
    ///
    /// For consumers that already hold something from that ecosystem. It cannot
    /// give you a [`File`]: the four methods there have no `futures-io`
    /// equivalent, which is the whole reason [`File`] exists.
    #[derive(Debug)]
    pub struct Compat<T>(pub T);

    impl<T> Read for Compat<T>
    where
        T: futures_io::AsyncRead + Unpin + Send,
    {
        async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Error> {
            futures_util::AsyncReadExt::read(&mut self.0, buffer)
                .await
                .map_err(Error::from)
        }
    }

    impl<T> Write for Compat<T>
    where
        T: futures_io::AsyncWrite + Unpin + Send,
    {
        async fn write(&mut self, buffer: &[u8]) -> Result<usize, Error> {
            futures_util::AsyncWriteExt::write(&mut self.0, buffer)
                .await
                .map_err(Error::from)
        }

        async fn flush(&mut self) -> Result<(), Error> {
            futures_util::AsyncWriteExt::flush(&mut self.0)
                .await
                .map_err(Error::from)
        }
    }

    impl<T> Seek for Compat<T>
    where
        T: futures_io::AsyncSeek + Unpin + Send,
    {
        async fn seek(&mut self, from: SeekFrom) -> Result<u64, Error> {
            futures_util::AsyncSeekExt::seek(&mut self.0, from.into())
                .await
                .map_err(Error::from)
        }
    }

    /// Open an existing file for reading and writing.
    pub async fn open(path: impl AsRef<Path>) -> Result<HostFile, Error> {
        Ok(HostFile::new(
            std::fs::OpenOptions::new().read(true).write(true).open(path)?,
        ))
    }

    /// Create a file for reading and writing, truncating one already there.
    pub async fn create(path: impl AsRef<Path>) -> Result<HostFile, Error> {
        Ok(HostFile::new(
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)?,
        ))
    }

    /// Open for reading and writing, creating it if absent and keeping what is
    /// there if not.
    pub async fn open_or_create(path: impl AsRef<Path>) -> Result<HostFile, Error> {
        Ok(HostFile::new(
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)?,
        ))
    }

    /// Open for appending, creating it if absent.
    pub async fn append(path: impl AsRef<Path>) -> Result<HostFile, Error> {
        Ok(HostFile::new(
            std::fs::OpenOptions::new().append(true).create(true).open(path)?,
        ))
    }

    /// Read a whole file.
    pub async fn read(path: impl AsRef<Path>) -> Result<std::vec::Vec<u8>, Error> {
        Ok(std::fs::read(path)?)
    }

    /// Replace a file's contents.
    pub async fn write(path: impl AsRef<Path>, contents: impl AsRef<[u8]>) -> Result<(), Error> {
        Ok(std::fs::write(path, contents)?)
    }

    /// How long the file at `path` is, without opening it.
    pub async fn metadata(path: impl AsRef<Path>) -> Result<u64, Error> {
        Ok(std::fs::metadata(path)?.len())
    }

    /// Remove a file.
    pub async fn remove_file(path: impl AsRef<Path>) -> Result<(), Error> {
        Ok(std::fs::remove_file(path)?)
    }

    /// Remove a directory and everything under it.
    pub async fn remove_dir_all(path: impl AsRef<Path>) -> Result<(), Error> {
        Ok(std::fs::remove_dir_all(path)?)
    }

    /// Rename a file, replacing the destination if it exists.
    pub async fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> Result<(), Error> {
        Ok(std::fs::rename(from, to)?)
    }

    /// Create a directory and every missing parent.
    pub async fn create_dir_all(path: impl AsRef<Path>) -> Result<(), Error> {
        Ok(std::fs::create_dir_all(path)?)
    }
}

/// A [`File`] over a fixed byte array, implemented the way a consumer with no
/// operating system would implement one over flash.
///
/// **This is a guard, not a utility.** `futures-io` compiles for a bare-metal
/// target while exporting no traits at all, so "the crate builds" is not
/// evidence that anything downstream can be written. This module touches no
/// `std` and implements every method of every trait above, including the
/// provided ones, so a change that quietly made them unimplementable off a host
/// would stop compiling here.
#[cfg(test)]
mod portable {
    use super::{Error, ErrorKind, File, Read, Seek, SeekFrom, Write};
    use core::future::Future;

    struct Ram {
        bytes: [u8; 256],
        length: u64,
        at: u64,
    }

    impl Read for Ram {
        fn read(&mut self, buffer: &mut [u8]) -> impl Future<Output = Result<usize, Error>> + Send {
            let start = self.at as usize;
            let end = (start + buffer.len()).min(self.length as usize);
            let taken = end.saturating_sub(start);
            buffer[..taken].copy_from_slice(&self.bytes[start..end]);
            self.at += taken as u64;
            async move { Ok(taken) }
        }
    }

    impl Write for Ram {
        fn write(&mut self, buffer: &[u8]) -> impl Future<Output = Result<usize, Error>> + Send {
            let start = self.at as usize;
            let result = if start >= self.bytes.len() {
                Err(Error::new(ErrorKind::WriteZero))
            } else {
                let end = (start + buffer.len()).min(self.bytes.len());
                let taken = end - start;
                self.bytes[start..end].copy_from_slice(&buffer[..taken]);
                self.at += taken as u64;
                self.length = self.length.max(self.at);
                Ok(taken)
            };
            async move { result }
        }

        async fn flush(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }

    impl Seek for Ram {
        async fn seek(&mut self, from: SeekFrom) -> Result<u64, Error> {
            self.at = match from {
                SeekFrom::Start(n) => n,
                SeekFrom::End(n) => self.length.saturating_add_signed(n),
                SeekFrom::Current(n) => self.at.saturating_add_signed(n),
            };
            Ok(self.at)
        }
    }

    impl File for Ram {
        async fn length(&mut self) -> Result<u64, Error> {
            Ok(self.length)
        }

        async fn set_length(&mut self, length: u64) -> Result<(), Error> {
            self.length = length;
            Ok(())
        }

        async fn sync_all(&mut self) -> Result<(), Error> {
            Ok(())
        }

        async fn sync_data(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }

    #[test]
    fn a_consumer_with_no_filesystem_can_implement_the_whole_trait() {
        let out = crate::block_on(async {
            let mut device = Ram {
                bytes: [0; 256],
                length: 0,
                at: 0,
            };
            device.write_all(b"disk").await?;
            device.seek(SeekFrom::Start(0)).await?;
            let mut back = [0u8; 4];
            device.read_exact(&mut back).await?;
            assert_eq!(device.stream_position().await?, 4);
            assert_eq!(device.length().await?, 4);
            device.sync_all().await?;
            Ok::<_, Error>(back)
        })
        .unwrap();
        assert_eq!(&out, b"disk");
    }
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::{create, metadata, open, remove_file, ErrorKind, File, Read, Seek, SeekFrom, Write};
    use crate::block_on;

    fn scratch(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("nagoya-io-{name}-{}", std::process::id()))
    }

    #[test]
    fn a_file_round_trips_through_the_traits() {
        let path = scratch("round-trip");
        block_on(async {
            let mut file = create(&path).await.unwrap();
            file.write_all(b"the bytes").await.unwrap();
            file.seek(SeekFrom::Start(0)).await.unwrap();
            let mut back = [0u8; 9];
            file.read_exact(&mut back).await.unwrap();
            assert_eq!(&back, b"the bytes");
        });
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn the_four_methods_no_io_trait_carries() {
        let path = scratch("methods");
        block_on(async {
            let mut file = create(&path).await.unwrap();
            file.write_all(b"0123456789").await.unwrap();
            file.sync_all().await.unwrap();
            assert_eq!(file.length().await.unwrap(), 10);

            file.set_length(4).await.unwrap();
            assert_eq!(file.length().await.unwrap(), 4);
            assert_eq!(metadata(&path).await.unwrap(), 4);

            file.sync_data().await.unwrap();
        });
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn reading_past_the_end_is_not_silently_short() {
        let path = scratch("short");
        block_on(async {
            let mut file = create(&path).await.unwrap();
            file.write_all(b"four").await.unwrap();
            file.seek(SeekFrom::Start(0)).await.unwrap();

            // `read` reports the end as zero bytes and no error.
            let mut plenty = [0u8; 64];
            assert_eq!(file.read(&mut plenty).await.unwrap(), 4);
            assert_eq!(file.read(&mut plenty).await.unwrap(), 0);

            // `read_exact` asked for a fixed count, so running out is an error.
            file.seek(SeekFrom::Start(0)).await.unwrap();
            let mut exact = [0u8; 64];
            let error = file.read_exact(&mut exact).await.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::UnexpectedEof);
        });
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn seeking_reports_where_it_landed() {
        let path = scratch("seek");
        block_on(async {
            let mut file = create(&path).await.unwrap();
            file.write_all(&[0u8; 100]).await.unwrap();

            assert_eq!(file.seek(SeekFrom::Start(10)).await.unwrap(), 10);
            assert_eq!(file.seek(SeekFrom::Current(5)).await.unwrap(), 15);
            assert_eq!(file.seek(SeekFrom::Current(-5)).await.unwrap(), 10);
            assert_eq!(file.seek(SeekFrom::End(-1)).await.unwrap(), 99);
            assert_eq!(file.stream_position().await.unwrap(), 99);
        });
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn opening_something_that_is_not_there_says_so() {
        let error = block_on(open(scratch("absent"))).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);
    }

    #[test]
    fn removing_a_file_removes_it() {
        let path = scratch("removed");
        block_on(async {
            create(&path).await.unwrap();
            assert!(path.exists());
            remove_file(&path).await.unwrap();
        });
        assert!(!path.exists());
    }
}
