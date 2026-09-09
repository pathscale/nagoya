//! Files, for consumers that need one and do not want a runtime attached.
//!
//! # Why this is here and not a reactor
//!
//! The crate documentation says there will be no I/O driver, and that still
//! holds: an epoll or io_uring reactor is the part that needs an operating
//! system, and being able to run without one is the point of this crate.
//!
//! **A file abstraction is not a reactor.** It is a trait plus one
//! implementation for hosts that have `std`, exactly the shape [`Host`] already
//! has for parking. A consumer on bare metal implements [`File`] over its own
//! flash or block device and never links the module below.
//!
//! # Why it exists at all
//!
//! Because the operations do not fit in any existing trait and every storage
//! crate therefore reinvents them. `futures-io` covers reading, writing and
//! seeking one file, and nothing covers `sync_all`, `set_len`, `metadata`, or
//! opening one in the first place. WorkTable had these in a private module and
//! `data_bucket` could not name them at all, so the two halves of one storage
//! engine disagreed about what a file was.
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
//!
//! # Why the whole module needs `std`, for now
//!
//! **`futures-io` puts every one of its traits behind its own `std` feature.**
//! Turn that off and the crate compiles to nothing at all: no `AsyncRead`, no
//! `AsyncWrite`, no `AsyncSeek`. They take `std::io::Error` and `IoSlice`, so
//! there was nowhere else for them to go.
//!
//! That is worth stating plainly because it is easy to check the wrong way. The
//! crate *builds* for `aarch64-unknown-none`, so a probe that only compiles it
//! reports success; it is the exports that vanish. Any `File` trait written on
//! top of those three is a `std` trait wearing a portable name, which includes
//! `data_bucket`'s `AsyncFile`.
//!
//! A file trait that works without an operating system therefore needs its own
//! read, write and seek, over an error type that is not `std::io::Error`. That
//! is a deliberate piece of design and it is not done here. What is here is the
//! shared home for the operations, so `worktable` and `data_bucket` stop
//! disagreeing about what a file is while it gets done.

use core::future::Future;

/// A file this crate can read, write and seek, without naming whose runtime
/// owns it.
///
/// The three supertraits come from `futures-io` because they belong to no
/// runtime and are `no_std`-safe with their `std` feature off. The four methods
/// are the ones no I/O trait carries.
pub trait File: futures_io::AsyncRead + futures_io::AsyncWrite + futures_io::AsyncSeek + Unpin {
    /// Bytes currently in the file.
    fn length(&self) -> impl Future<Output = Result<u64, Error>> + Send;

    /// Truncate or extend to `length`, zero-filling any new bytes.
    fn set_length(&mut self, length: u64) -> impl Future<Output = Result<(), Error>> + Send;

    /// Flush this file's contents *and* its metadata to the storage device.
    fn sync_all(&mut self) -> impl Future<Output = Result<(), Error>> + Send;

    /// Flush this file's contents, leaving metadata to the platform.
    ///
    /// Cheaper than [`File::sync_all`] where the platform distinguishes them,
    /// and identical to it where it does not.
    fn sync_data(&mut self) -> impl Future<Output = Result<(), Error>> + Send;
}

/// What went wrong.
///
/// Deliberately not `std::io::Error`: this trait has to be implementable on a
/// target that has no `std`. With `std` on, the two convert.
#[derive(Debug)]
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
    /// Anything else the platform reported.
    Other,
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let text = match self.kind {
            ErrorKind::NotFound => "no such file",
            ErrorKind::PermissionDenied => "permission denied",
            ErrorKind::UnexpectedEof => "unexpected end of file",
            ErrorKind::Other => "file operation failed",
        };
        f.write_str(text)
    }
}

#[cfg(feature = "std")]
impl std::error::Error for Error {}

#[cfg(feature = "std")]
impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::new(match error.kind() {
            std::io::ErrorKind::NotFound => ErrorKind::NotFound,
            std::io::ErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
            std::io::ErrorKind::UnexpectedEof => ErrorKind::UnexpectedEof,
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
                ErrorKind::Other => std::io::ErrorKind::Other,
            },
            error,
        )
    }
}

#[cfg(feature = "std")]
pub use host::{
    append, create, create_dir_all, metadata, open, open_or_create, read, remove_dir_all,
    remove_file, rename, write, HostFile,
};

#[cfg(feature = "std")]
mod host {
    //! The implementation for a target that has a filesystem.

    use super::{Error, File};
    use core::future::Future;
    use std::path::Path;

    /// A [`File`] backed by `std::fs`, carrying the `futures-io` traits through
    /// `AllowStdIo`.
    ///
    /// `AllowStdIo` and not `async-fs`: `async-fs` keeps a user-space write
    /// buffer and flushes it when the handle drops, best effort and silently.
    /// Its own documentation says errors detected on closing are ignored. A
    /// storage engine that drops a handle then reads the file back gets its
    /// last writes missing and no error, which is how it was found.
    pub type HostFile = futures_util::io::AllowStdIo<std::fs::File>;

    impl File for HostFile {
        fn length(&self) -> impl Future<Output = Result<u64, Error>> + Send {
            let result = self.get_ref().metadata().map(|m| m.len()).map_err(Error::from);
            async move { result }
        }

        fn set_length(&mut self, length: u64) -> impl Future<Output = Result<(), Error>> + Send {
            let result = self.get_ref().set_len(length).map_err(Error::from);
            async move { result }
        }

        fn sync_all(&mut self) -> impl Future<Output = Result<(), Error>> + Send {
            let result = self.get_ref().sync_all().map_err(Error::from);
            async move { result }
        }

        fn sync_data(&mut self) -> impl Future<Output = Result<(), Error>> + Send {
            let result = self.get_ref().sync_data().map_err(Error::from);
            async move { result }
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

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::{create, metadata, open, remove_file, File};
    use crate::block_on;
    use futures_util::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
    use std::io::SeekFrom;

    fn scratch(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("nagoya-io-{name}-{}", std::process::id()))
    }

    #[test]
    fn a_file_round_trips_through_the_futures_traits() {
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
            // `length` reads the file, not the buffer, so it has to see the
            // write without a flush of its own.
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
    fn opening_something_that_is_not_there_says_so() {
        let error = block_on(open(scratch("absent"))).unwrap_err();
        assert_eq!(error.kind(), super::ErrorKind::NotFound);
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
