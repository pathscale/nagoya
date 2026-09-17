//! Readiness in, wakers out: the I/O driver, for hosts that have an OS.
//!
//! # Why this exists in a crate that runs without an operating system
//!
//! Because needing an OS is not the same as needing one *always*.
//! [`runtime`](crate::runtime) owns threads, which `no_std` cannot have, and
//! resolves it by living behind the `std` feature: "off without `std`, and
//! that is the honest outcome". This module is the same shape. A `no_std`
//! build has no sockets and says so by not compiling, rather than by pretending
//! the problem is absent.
//!
//! The rule it replaces was "a caller that wants sockets brings its own
//! reactor". That did not avoid the dependency, it multiplied it. Generational
//! tokens, edge-triggered registration, and integrating a timer wheel without
//! polling it are the same problem every time, and the ways of getting them
//! wrong are quiet ones: a readiness event in flight when a registration is
//! dropped wakes whatever task later took that slot, or a busy socket takes the
//! timer heap's lock on every single event to be told nothing is due. Solving
//! that once, here, is the point.
//!
//! # No `select!`, anywhere
//!
//! Nothing here polls a set of sources to ask which is ready. Each descriptor
//! owns its own waker slot, and readiness wakes exactly the task parked on that
//! slot. A combinator that takes N futures and polls all of them on every
//! wakeup is the thing this replaces: it costs N polls per event and couples
//! sources that have nothing to do with each other.
//!
//! The timer integration is the clearest case. The next deadline is cached, so
//! a socket delivering ten thousand events a second never touches the timer
//! heap's lock to be told that nothing is due. Socket traffic and timer work
//! are independent: neither makes the other do any work. `select!` cannot
//! express that, because it folds every branch into one poll set.
//!
//! # What a caller gets
//!
//! [`Reactor`] owns the poller and a thread; [`Handle`] registers descriptors
//! with it; [`Registration`] parks a waker for one descriptor's readability or
//! writability. [`TcpStream`] and [`TcpListener`] are the ordinary sockets
//! built on that, and [`TcpStream`] implements [`crate::io::Stream`], which
//! this crate declared without providing an implementation.
//!
//! [`block_on`] is the other arrangement: no reactor thread at all, one thread
//! that waits for readiness and then polls the future itself. The handoff a
//! reactor thread costs is real, about ten microseconds a message, and a
//! server built as a thread per core with its own poller and its own
//! connections never needs to pay it.

#[cfg(feature = "syscall-counters")]
pub mod counters;
pub mod driver;
pub mod error;
pub mod local;
pub mod net;
pub mod poller;
pub mod socket;
#[cfg(test)]
mod testing;

pub use driver::{Handle, Reactor, Registration, Sharded};
pub use error::{Errno, Result};
pub use local::{block_on, block_on_with};
pub use net::{TcpListener, TcpStream};
pub use poller::{Event, Interest, Poller};
pub use socket::Addr;
