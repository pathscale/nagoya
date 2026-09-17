//! Files, for consumers that need one and do not want a runtime attached.
//!
//! # Why this is here and not a reactor
//!
//! The crate documentation says there will be no I/O driver, and that still
//! holds: an epoll or io_uring reactor is a large amount of platform, and not
//! linking one is the point of this crate.
//!
//! **A file abstraction is not a reactor.** It is a set of traits plus one
//! implementation over `std::fs`, exactly the shape [`Host`] already has for
//! parking: a consumer that does not want the `std` one supplies its own.
//!
//! [`Host`]: st3::fanout::Host
//!
//! # Why these traits and not `futures-io`
//!
//! Because `futures-io` cannot be used without `std`, and it is easy to
//! conclude otherwise. **Every one of its traits sits behind its own `std`
//! feature.** Turn that off and the crate compiles to nothing at all: no
//! `AsyncRead`, no `AsyncWrite`, no `AsyncSeek`. They take `std::io::Error` and
//! `IoSlice`, so there was nowhere else for them to go.
//!
//! That is worth stating plainly because checking it the obvious way gives the
//! wrong answer: the crate still *compiles* with the feature off, so a probe
//! that only builds it reports success, and it is the exports that vanish.
//! Anything written on top of those three is a `std` trait wearing a portable
//! name.
//!
//! So the traits below are the same shape with two differences that matter: the
//! error is [`Error`] rather than `std::io::Error`, and the seek origin is
//! [`SeekFrom`] rather than `std::io::SeekFrom`. With `std` on, the two
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
/// `std::io::SeekFrom` in all but name, restated because that one is in `std`
/// and this is not.
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
                match self.read(&mut buffer[filled..]).await {
                    Ok(0) => return Err(Error::new(ErrorKind::UnexpectedEof)),
                    Ok(n) => filled += n,
                    Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                    Err(error) => return Err(error),
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
                    Err(error) if error.kind() == ErrorKind::Interrupted => {
                        buffer.truncate(filled);
                    }
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
                match self.write(&buffer[written..]).await {
                    // Nothing taken and no error: the destination will not
                    // accept more, and looping would spin forever.
                    Ok(0) => return Err(Error::new(ErrorKind::WriteZero)),
                    Ok(n) => written += n,
                    Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                    Err(error) => return Err(error),
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

/// A byte stream: somewhere to read bytes from and write them to.
///
/// # Why this sits beside [`Read`] and [`Write`] rather than reusing them
///
/// Those describe a file, and two of their properties are wrong for a socket.
///
/// Their futures are `Send`, because a file handed to a worker pool has to
/// cross threads. A socket driven by a reactor often must not be: a consumer
/// holding non-`Send` state across an await needs the future to stay on one
/// thread, and requiring `Send` forbids that outright. That is not a niche
/// case, it is how a `spawn_local` dispatch model works.
///
/// And [`ErrorKind`] has nowhere to say "not ready yet". For a file that is
/// not a state that exists; for a non-blocking socket it is the ordinary one,
/// and it is the signal a reactor turns on. [`StreamError`] carries the
/// platform's own code, so it can.
///
/// # Why this belongs here rather than in whatever crate needs it
///
/// Because two crates always need it and neither should depend on the other.
/// A TLS session implements this so a protocol can run over it; a protocol
/// consumes it so it need not care whether TLS is underneath. Whichever of
/// them owns the definition, the other depends on it for a reason unrelated
/// to what it does, and that is the shape of mistake `tokio-rustls` is stuck
/// with: `AsyncRead` lives in `tokio`, so the glue drags the whole runtime
/// along.
///
/// This crate is already the common dependency, which makes it the right place
/// for the definition. It was written here while there was no I/O driver to
/// implement it; [`reactor::TcpStream`](crate::reactor::TcpStream) implements
/// it now, and the trait is the better for having been designed before there
/// was a socket to shape it around.
#[allow(async_fn_in_trait)]
pub trait Stream {
    /// Read into `buffer`, returning how many bytes arrived.
    ///
    /// A return of zero means the end of the stream, and is not an error.
    async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, StreamError>;

    /// Write the whole of `buffer`.
    ///
    /// All of it, rather than reporting a count: a partial write is a property
    /// of the transport rather than something a protocol wants to reason
    /// about, and every caller would otherwise write the same loop.
    async fn write_all(&mut self, buffer: &[u8]) -> Result<(), StreamError>;
}

/// What went wrong on a [`Stream`].
///
/// The platform's own code rather than a mapped enum. A socket consumer asks
/// questions about specific numbers that a coarse set cannot answer, and the
/// two it always asks have methods here so the common path is not a
/// comparison against a literal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamError(pub i32);

impl StreamError {
    /// `EAGAIN` on this target.
    ///
    /// Spelled out per platform because this crate links no libc to ask, and
    /// the number differs: 11 on Linux, 35 on the BSDs and macOS. A consumer
    /// that does have libc should build the value from its own constant; this
    /// exists for one that does not.
    pub const WOULD_BLOCK: Self = Self(if cfg!(target_os = "linux") { 11 } else { 35 });

    /// `EINTR`, which is 4 on every target this supports.
    pub const INTERRUPTED: Self = Self(4);

    /// Whether this means "nothing to do yet" rather than a failure.
    ///
    /// The one question a reactor asks of every read and every write. Both
    /// numberings are checked rather than only this platform's: POSIX allows
    /// `EAGAIN` and `EWOULDBLOCK` to differ and does not say which a given
    /// call returns.
    #[must_use]
    pub const fn would_block(self) -> bool {
        self.0 == 11 || self.0 == 35
    }

    /// Whether a signal cut the call short and it can be retried as-is.
    #[must_use]
    pub const fn interrupted(self) -> bool {
        self.0 == Self::INTERRUPTED.0
    }
}

impl core::fmt::Display for StreamError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(formatter, "errno {}", self.0)
    }
}

#[cfg(feature = "std")]
impl std::error::Error for StreamError {}

#[cfg(feature = "std")]
impl From<StreamError> for std::io::Error {
    fn from(value: StreamError) -> Self {
        Self::from_raw_os_error(value.0)
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
// feature. A consumer building without `std` still wants `?` to work against
// its own error type, and gating this would have made the trait unusable in
// exactly the place it exists for.
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
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)?,
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
            std::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(path)?,
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

/// A [`File`] over a fixed byte array, implemented the way a consumer that does
/// not want the `std` one would implement it.
///
/// **This is a guard, not a utility.** `futures-io` still compiles with its
/// `std` feature off while exporting no traits at all, so "the crate builds" is
/// not evidence that anything downstream can be written. This module touches no
/// `std` and implements every method of every trait above, including the
/// provided ones, so a change that quietly made them unimplementable without
/// `std` would stop compiling here.
#[cfg(test)]
mod portable {
    use super::{Stream, StreamError};

    /// A stream over two buffers, to show the trait is implementable with no
    /// platform underneath it and no `Send` bound to satisfy.
    struct Pair<'a> {
        incoming: &'a [u8],
        outgoing: alloc::vec::Vec<u8>,
    }

    impl Stream for Pair<'_> {
        async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, StreamError> {
            let take = self.incoming.len().min(buffer.len());
            buffer[..take].copy_from_slice(&self.incoming[..take]);
            self.incoming = &self.incoming[take..];
            Ok(take)
        }

        async fn write_all(&mut self, buffer: &[u8]) -> Result<(), StreamError> {
            self.outgoing.extend_from_slice(buffer);
            Ok(())
        }
    }

    #[test]
    fn a_stream_needs_no_platform() {
        let mut pair = Pair {
            incoming: b"hello",
            outgoing: alloc::vec::Vec::new(),
        };
        let read = crate::block_on(async {
            let mut buffer = [0u8; 5];
            let read = pair.read(&mut buffer).await.expect("read");
            pair.write_all(&buffer[..read]).await.expect("write");
            read
        });
        assert_eq!(read, 5);
        assert_eq!(pair.outgoing, b"hello");

        // The end of a stream is zero rather than an error.
        let rest = crate::block_on(async {
            let mut buffer = [0u8; 4];
            pair.read(&mut buffer).await.expect("read")
        });
        assert_eq!(rest, 0);
    }

    #[test]
    fn would_block_is_recognised_from_either_numbering() {
        // POSIX allows EAGAIN and EWOULDBLOCK to differ and does not say which
        // a call returns, so both are accepted.
        assert!(StreamError(11).would_block());
        assert!(StreamError(35).would_block());
        assert!(StreamError::WOULD_BLOCK.would_block());
        assert!(!StreamError(5).would_block());
        assert!(StreamError::INTERRUPTED.interrupted());
        assert!(!StreamError::WOULD_BLOCK.interrupted());
    }

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
    use super::{
        create, metadata, open, remove_file, ErrorKind, File, Read, Seek, SeekFrom, Write,
    };
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
