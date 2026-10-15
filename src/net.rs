//! Non-blocking TCP and UDP over the reactor.
//!
//! # No `std::net`
//!
//! The sockets come from [`socket`](super::socket), which is libc and nothing
//! else. `std::net` was not slow - measured against raw `send`/`recv` the
//! difference was inside the noise - but its interface is the wrong shape for
//! a reactor and it was the last thing in this crate reaching for `std`.
//!
//! What that interface got wrong, concretely: non-blocking was a hidden mode
//! rather than a type, so nothing stopped a blocking read on the reactor
//! thread; `WouldBlock` arrived as an `io::Error` when "not ready" is the
//! ordinary state of a reactive socket; and `io::Read::read` demanded
//! initialised memory, which cost 0.15us per read zeroing bytes the kernel
//! immediately overwrote.
//!
//! # The edge triggered contract
//!
//! Readiness is parked only after the kernel says `EWOULDBLOCK`. A TCP read
//! can return after filling its slice, so it restores readiness when data may
//! remain. A UDP read consumes one datagram and restores readiness for the
//! next poll; only an attempted receive that would block proves the queue is
//! drained. Registering interest without either step would wait for an edge
//! that has already passed, and the task would hang with data in the socket.

// Reading into uninitialised memory is the one thing this module does that the
// safe subset cannot express; see `poll_read_buf`.
#![allow(unsafe_code)]

use core::pin::Pin;
use core::task::{Context, Poll};

use crate::reactor::driver::{Handle, Registration};
use crate::reactor::error::Result;
use crate::reactor::poller::Interest;
use crate::reactor::socket::{
    Addr, TcpListener as Listener, TcpSocket, UdpSocket as DatagramSocket,
};

/// Name resolution, which tokio also puts under `net`.
///
/// [`lookup_host`] is [`crate::resolve::resolve`] under the name tokio gives
/// it. The two differ in a way the name does not warn about and the function's
/// own documentation does: tokio's is an `async fn` its runtime resolves on a
/// blocking pool, and this one is a plain `fn` that calls `getaddrinfo` on the
/// calling thread, because this crate has no blocking pool to hand it to.
/// Resolve before entering the reactor, or the lookup stalls every socket on
/// it.
pub use crate::resolve::{ResolveError, connect_any, resolve as lookup_host, resolve};

/// A connection that yields to the executor instead of blocking.
#[derive(Debug)]
pub struct TcpStream {
    inner: TcpSocket,
    registration: Registration,
    /// Whether a connect is still in flight and [`Connected`] has work to do.
    ///
    /// False for an adopted socket, which is connected by definition, and for
    /// a Unix-domain connect, which finishes inside the `connect` call rather
    /// than by making the socket writable later. Asking anyway is not merely
    /// wasteful there, it hangs: the test is `getpeername`, which starts
    /// failing with `ENOTCONN` the moment the peer closes, and the writable
    /// edge it then waits for went by before the descriptor was registered.
    connecting: bool,
    /// The send buffer's capacity, or zero where the kernel would not say.
    ///
    /// Read once here rather than per write. It answers one question that the
    /// return value of a write cannot: a write the kernel took whole usually
    /// means there is room left, but not when the caller handed down exactly
    /// as much as the buffer holds, and a stream copier sized to the buffer
    /// does that on every single write.
    send_capacity: usize,
}

impl TcpStream {
    /// Adopt an already connected socket.
    ///
    /// The socket is non-blocking from birth and has Nagle off already; this
    /// only has to register it.
    pub fn from_socket(socket: TcpSocket, handle: &Handle) -> Result<Self> {
        let registration = handle.register(socket.raw(), Interest::BOTH)?;
        let send_capacity = socket.send_buffer().unwrap_or(0);
        Ok(Self {
            inner: socket,
            registration,
            connecting: false,
            send_capacity,
        })
    }

    /// Connect to `addr`, returning once the handshake has completed.
    ///
    /// The socket is non-blocking, so the kernel's `connect` returns
    /// immediately with `EINPROGRESS` and signals completion by making the
    /// socket writable. Awaiting that here rather than handing it to the caller
    /// is deliberate: a stream that is connected only eventually is a trap,
    /// because the first write appears to succeed into the socket buffer and
    /// the failure surfaces somewhere unrelated.
    pub async fn connect(addr: Addr, handle: &Handle) -> Result<Self> {
        let mut stream = Self::connect_started(addr, handle)?;
        Connected {
            stream: &mut stream,
        }
        .await?;
        Ok(stream)
    }

    /// Start connecting without waiting for the handshake.
    ///
    /// For a caller that wants to overlap the wait with other work. It must
    /// await [`Self::connected`] before treating the stream as usable.
    ///
    /// For a Unix-domain address there is nothing to overlap: the connect has
    /// already succeeded or already failed by the time this returns, and
    /// [`Self::connected`] resolves immediately.
    pub fn connect_started(addr: Addr, handle: &Handle) -> Result<Self> {
        let socket = TcpSocket::connect(addr)?;
        let mut stream = Self::from_socket(socket, handle)?;
        // A Unix-domain connect does not go in flight, so there is no later
        // answer to wait for and waiting would never end.
        stream.connecting = !matches!(addr, Addr::Path(_));
        Ok(stream)
    }

    /// Wait for an in-flight connect to finish.
    ///
    /// A non-blocking connect reports failure by making the socket writable and
    /// leaving the reason in `SO_ERROR`, which is indistinguishable from
    /// success without asking. This asks.
    pub async fn connected(&mut self) -> Result<()> {
        Connected { stream: self }.await
    }

    /// The peer's address.
    pub fn peer_addr(&self) -> Result<Addr> {
        self.inner.peer_addr()
    }

    /// This socket's own address.
    pub fn local_addr(&self) -> Result<Addr> {
        self.inner.local_addr()
    }

    /// Read straight into a `BytesMut`'s spare capacity.
    ///
    /// The obvious way to fill a growable buffer is to read into a stack array
    /// and copy, and that copies every byte received for no reason. This reads
    /// into the uninitialised tail and then declares how much arrived, so the
    /// bytes land where they are going to be parsed.
    ///
    /// `buffer` must have spare capacity; a full buffer reads nothing and
    /// returns `Ok(0)`, which the caller would misread as end of stream.
    pub fn poll_read_buf(
        &mut self,
        cx: &mut Context<'_>,
        buffer: &mut bytes::BytesMut,
    ) -> Poll<Result<usize>> {
        let spare = buffer.spare_capacity_mut();
        if spare.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let spare_len = spare.len();
        let spare_ptr = spare.as_mut_ptr();
        let filled = buffer.len();

        if !self.registration.take_readable_or_park(cx.waker()) {
            return Poll::Pending;
        }
        loop {
            // SAFETY: `spare_ptr` points at `spare_len` bytes of allocated
            // capacity owned by `buffer`, which outlives this call, and `recv`
            // only writes within the length it is given. The spare region is
            // beyond the buffer's length, so no live reference overlaps it.
            let result = unsafe { self.inner.recv(spare_ptr.cast::<u8>(), spare_len) };

            match result {
                Ok(read) => {
                    // SAFETY: `recv` reported writing `read` bytes into the
                    // spare capacity, so that many past `filled` are live.
                    unsafe { buffer.set_len(filled + read) };
                    // As in `poll_read`: a filled buffer is not evidence of
                    // more, so ask instead of assuming.
                    if read == spare_len && self.inner.pending().is_none_or(|left| left > 0) {
                        self.registration.mark_readable();
                    }
                    return Poll::Ready(Ok(read));
                }
                Err(error) if error.would_block() => {
                    self.registration.poll_readable(cx.waker());
                    return Poll::Pending;
                }
                Err(error) if error.interrupted() => continue,
                Err(error) => return Poll::Ready(Err(error)),
            }
        }
    }

    /// Read into `buffer`, parking `cx`'s waker if the socket would block.
    pub fn poll_read(&mut self, cx: &mut Context<'_>, buffer: &mut [u8]) -> Poll<Result<usize>> {
        // Nothing has arrived since the last read drained this socket, so the
        // `recv` below would return `EWOULDBLOCK`. The waker is parked by the
        // same call that answered, so an edge cannot slip in between.
        if !self.registration.take_readable_or_park(cx.waker()) {
            return Poll::Pending;
        }
        loop {
            // SAFETY: `buffer` is a live initialised slice, so writing up to
            // its length into it is in bounds.
            let result = unsafe { self.inner.recv(buffer.as_mut_ptr(), buffer.len()) };
            match result {
                Ok(n) => {
                    // A read that filled the buffer has not proved the socket
                    // empty, so readiness is put back rather than consumed.
                    //
                    // Asking the kernel what is actually left, rather than
                    // assuming a full buffer means more: a framed reader calls
                    // `read_exact`, so every one of its reads fills its buffer
                    // exactly and the assumption is wrong every time. That cost
                    // one `recv` returning `EWOULDBLOCK` per read, measured, on
                    // the path a WebSocket server takes several times a frame.
                    // `FIONREAD` is 123ns against that call's 176ns here, and
                    // unlike it this one is only made on the exact-fill path.
                    if n == buffer.len() && self.inner.pending().is_none_or(|left| left > 0) {
                        self.registration.mark_readable();
                    }
                    return Poll::Ready(Ok(n));
                }
                Err(error) if error.would_block() => {
                    // Drained: now it is safe to wait for the next edge.
                    self.registration.poll_readable(cx.waker());
                    return Poll::Pending;
                }
                // A signal interrupted the read; the data is still there.
                Err(error) if error.interrupted() => continue,
                Err(error) => return Poll::Ready(Err(error)),
            }
        }
    }

    /// Write from `buffer`, parking `cx`'s waker if the socket would block.
    pub fn poll_write(&mut self, cx: &mut Context<'_>, buffer: &[u8]) -> Poll<Result<usize>> {
        self.poll_write_vectored(cx, &[], buffer)
    }

    /// Write two slices as one datagram to the kernel, without joining them.
    ///
    /// A WebSocket frame is a short header followed by a payload the caller
    /// already owns. Concatenating them to get one `write` copies the whole
    /// payload for the sake of at most fourteen leading bytes. `writev` hands
    /// the kernel both addresses instead: one syscall, one segment, no copy.
    pub fn poll_write_vectored(
        &mut self,
        cx: &mut Context<'_>,
        first: &[u8],
        second: &[u8],
    ) -> Poll<Result<usize>> {
        // The socket has had no room since the last write filled it, so the
        // `sendmsg` below would return `EWOULDBLOCK`. The waker is parked by
        // the same call that answered, so an edge cannot slip in between.
        if !self.registration.take_writable_or_park(cx.waker()) {
            return Poll::Pending;
        }
        let wanted = first.len() + second.len();
        loop {
            match self.inner.send_vectored(first, second) {
                Ok(n) => {
                    // A write the kernel took whole has not proved the buffer
                    // full, so readiness is put back rather than consumed. A
                    // short write has proved it, and that is the one case
                    // where the next write must wait for an edge.
                    //
                    // Learning this from the return value rather than from a
                    // second `sendmsg` written only to be rejected is worth
                    // half of every write on a saturated socket: the count was
                    // 256 `sendmsg` per MiB against a floor of 128.
                    // A write at least as large as the buffer that the kernel
                    // took whole has filled it, whatever the return value
                    // suggests, and saying otherwise costs a `sendmsg` written
                    // only to be rejected. A copier chunked to the buffer size
                    // hits this on every write.
                    if n == wanted && (self.send_capacity == 0 || wanted < self.send_capacity) {
                        self.registration.mark_writable();
                    }
                    return Poll::Ready(Ok(n));
                }
                Err(error) if error.would_block() => {
                    // Full: now it is safe to wait for the next edge.
                    self.registration.poll_writable(cx.waker());
                    return Poll::Pending;
                }
                Err(error) if error.interrupted() => continue,
                Err(error) => return Poll::Ready(Err(error)),
            }
        }
    }

    /// Write a header and a payload, looping until both are gone.
    ///
    /// The partial-write bookkeeping is the reason this is a method rather
    /// than something the caller assembles: a vectored write can stop anywhere,
    /// including part way through the header, and resuming it correctly means
    /// tracking which slice the remainder falls in.
    pub fn write_all_vectored<'a>(
        &'a mut self,
        header: &'a [u8],
        payload: &'a [u8],
    ) -> WriteAllVectored<'a> {
        WriteAllVectored {
            stream: self,
            header,
            payload,
            written: 0,
        }
    }

    /// Flush, which is a no-op for an unbuffered socket but completes the trait.
    pub fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    /// Read some bytes, as a future.
    pub fn read<'a>(&'a mut self, buffer: &'a mut [u8]) -> Read<'a> {
        Read {
            stream: self,
            buffer,
        }
    }

    /// Read into a `BytesMut`'s spare capacity, as a future.
    ///
    /// See [`Self::poll_read_buf`] for why this exists rather than reading into
    /// an array and copying.
    pub fn read_buf<'a>(&'a mut self, buffer: &'a mut bytes::BytesMut) -> ReadBuf<'a> {
        ReadBuf {
            stream: self,
            buffer,
        }
    }

    /// Write the whole of `buffer`, as a future.
    ///
    /// A partial write is normal on a socket whose send buffer filled, so this
    /// loops rather than returning a count the caller has to handle.
    pub fn write_all<'a>(&'a mut self, buffer: &'a [u8]) -> WriteAll<'a> {
        WriteAll {
            stream: self,
            buffer,
            written: 0,
        }
    }
}

/// The future returned by [`TcpStream::connected`].
#[derive(Debug)]
pub struct Connected<'a> {
    stream: &'a mut TcpStream,
}

impl core::future::Future for Connected<'_> {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // Nothing was ever in flight: an adopted socket, or a Unix-domain
        // connect that finished inside the `connect` call. The checks below
        // would not merely be redundant, they would be wrong, because
        // `getpeername` fails once a peer that was there has gone away.
        if !this.stream.connecting {
            return Poll::Ready(Ok(()));
        }

        // `SO_ERROR` is the authority on whether the handshake finished, but
        // it reads zero both for "succeeded" and for "still going", so it
        // cannot be the test on its own. `getpeername` distinguishes them: it
        // only succeeds once there is a peer, which is exactly the condition
        // being waited for.
        match this.stream.inner.connect_error() {
            Err(error) => return Poll::Ready(Err(error)),
            Ok(()) if this.stream.inner.peer_addr().is_ok() => {
                // Settled, so a second await of this is free rather than two
                // more syscalls.
                this.stream.connecting = false;
                return Poll::Ready(Ok(()));
            }
            Ok(()) => {}
        }

        // Still in flight: the kernel makes the socket writable when it is
        // done, either way.
        this.stream.registration.poll_writable(cx.waker());
        Poll::Pending
    }
}

/// The future returned by [`TcpStream::read`].
#[derive(Debug)]
pub struct Read<'a> {
    stream: &'a mut TcpStream,
    buffer: &'a mut [u8],
}

impl core::future::Future for Read<'_> {
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.stream.poll_read(cx, this.buffer)
    }
}

/// The future returned by [`TcpStream::write_all_vectored`].
#[derive(Debug)]
pub struct WriteAllVectored<'a> {
    stream: &'a mut TcpStream,
    header: &'a [u8],
    payload: &'a [u8],
    /// Bytes of `header + payload` already accepted by the socket.
    written: usize,
}

impl core::future::Future for WriteAllVectored<'_> {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let total = this.header.len() + this.payload.len();

        while this.written < total {
            // Where the remainder starts. Once the header is fully out the
            // first slice is empty and this degenerates to a plain write of
            // what is left of the payload.
            let (first, second) = if this.written < this.header.len() {
                (&this.header[this.written..], this.payload)
            } else {
                (&[][..], &this.payload[this.written - this.header.len()..])
            };

            match this.stream.poll_write_vectored(cx, first, second) {
                // A socket that accepts nothing is not going to start; the
                // peer has gone. Reported as a broken pipe, which is what the
                // next write would have produced anyway.
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(crate::reactor::error::codes::BROKEN_PIPE));
                }
                Poll::Ready(Ok(n)) => this.written += n,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
        }
        Poll::Ready(Ok(()))
    }
}

/// The future returned by [`TcpStream::read_buf`].
#[derive(Debug)]
pub struct ReadBuf<'a> {
    stream: &'a mut TcpStream,
    buffer: &'a mut bytes::BytesMut,
}

impl core::future::Future for ReadBuf<'_> {
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.stream.poll_read_buf(cx, this.buffer)
    }
}

/// The future returned by [`TcpStream::write_all`].
#[derive(Debug)]
pub struct WriteAll<'a> {
    stream: &'a mut TcpStream,
    buffer: &'a [u8],
    written: usize,
}

impl core::future::Future for WriteAll<'_> {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        while this.written < this.buffer.len() {
            match this.stream.poll_write(cx, &this.buffer[this.written..]) {
                // A socket that accepts nothing is not going to start; the
                // peer has gone. Reported as a broken pipe, which is what the
                // next write would have produced anyway.
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(crate::reactor::error::codes::BROKEN_PIPE));
                }
                Poll::Ready(Ok(n)) => this.written += n,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
        }
        Poll::Ready(Ok(()))
    }
}

/// A TCP listener that yields instead of blocking on `accept`.
#[derive(Debug)]
pub struct TcpListener {
    inner: Listener,
    registration: Registration,
    handle: Handle,
}

impl TcpListener {
    /// How many pending connections the kernel will hold before refusing.
    ///
    /// Generous, because the cost is kernel memory per listener rather than
    /// per connection, and a queue that overflows under a burst produces
    /// refused connections that look like a server fault.
    const BACKLOG: i32 = 1024;

    /// Bind to `addr` and register for incoming connections.
    pub fn bind(addr: Addr, handle: &Handle) -> Result<Self> {
        let listener = Listener::bind(addr, Self::BACKLOG)?;
        Self::from_listener(listener, handle)
    }

    /// Bind with `SO_REUSEPORT` and register, so that listeners on several
    /// reactors can share `addr`. See [`Listener::bind_shared`].
    pub fn bind_shared(addr: Addr, handle: &Handle) -> Result<Self> {
        let listener = Listener::bind_shared(addr, Self::BACKLOG)?;
        Self::from_listener(listener, handle)
    }

    /// Adopt an already bound listener.
    pub fn from_listener(listener: Listener, handle: &Handle) -> Result<Self> {
        let registration = handle.register(listener.raw(), Interest::READABLE)?;
        Ok(Self {
            inner: listener,
            registration,
            handle: handle.clone(),
        })
    }

    /// The address this listener is bound to.
    pub fn local_addr(&self) -> Result<Addr> {
        self.inner.local_addr()
    }

    /// Accept one connection, parking `cx`'s waker if none is waiting.
    pub fn poll_accept(&self, cx: &mut Context<'_>) -> Poll<Result<(TcpStream, Addr)>> {
        loop {
            if !self.registration.take_readable_or_park(cx.waker()) {
                return Poll::Pending;
            }
            match self.inner.accept() {
                Ok((socket, addr)) => {
                    // Keep draining queued connections until accept would block.
                    self.registration.mark_readable();
                    return Poll::Ready(
                        TcpStream::from_socket(socket, &self.handle).map(|stream| (stream, addr)),
                    );
                }
                Err(error) if error.would_block() => {
                    self.registration.poll_readable(cx.waker());
                    return Poll::Pending;
                }
                Err(error) if error.interrupted() => {
                    self.registration.mark_readable();
                    continue;
                }
                // A connection that died between the readiness event and the
                // accept is not this listener's problem: drop it and look for
                // the next one rather than failing the accept loop.
                Err(error) if crate::reactor::error::transient_accept(error) => {
                    self.registration.mark_readable();
                    continue;
                }
                Err(error) => return Poll::Ready(Err(error)),
            }
        }
    }

    /// Accept one connection, as a future.
    pub fn accept(&self) -> Accept<'_> {
        Accept { listener: self }
    }
}

/// The future returned by [`TcpListener::accept`].
#[derive(Debug)]
pub struct Accept<'a> {
    listener: &'a TcpListener,
}

impl core::future::Future for Accept<'_> {
    type Output = Result<(TcpStream, Addr)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.listener.poll_accept(cx)
    }
}

pub use crate::reactor::socket::Datagram;

/// An owned UDP socket driven by a nagoya reactor.
///
/// Each receive consumes one datagram. Successful operations retain readiness
/// until a subsequent syscall returns `WouldBlock`, including receives of
/// empty datagrams. UDP must not use TCP's FIONREAD optimisation: zero can
/// mean that an empty datagram is queued.
///
/// The receiver and sender have separate readiness slots, so one task may
/// receive while another sends on the same socket. There is one waker slot per
/// direction: serialize receives with each other and sends with each other.
/// Call [`Self::cancel_recv`] or [`Self::cancel_send`] when abandoning a
/// pending direct poll operation.
///
/// Dropping a pending receive or send future releases its waiter. A pending
/// receive has consumed nothing, and a pending send has sent nothing.
/// Dropping the socket deregisters it before closing the descriptor.
#[derive(Debug)]
pub struct UdpSocket {
    // Fields drop in declaration order. The registration must go first.
    registration: Registration,
    inner: DatagramSocket,
}

impl UdpSocket {
    /// Bind a non-blocking datagram socket and register it for reads and writes.
    pub fn bind(addr: Addr, handle: &Handle) -> Result<Self> {
        Self::bind_with_reuse(addr, handle, false)
    }

    /// Bind with `SO_REUSEADDR` enabled before claiming an IP address.
    pub fn bind_reuse_addr(addr: Addr, handle: &Handle) -> Result<Self> {
        Self::bind_with_reuse(addr, handle, true)
    }

    /// Bind with an explicit `SO_REUSEADDR` setting applied before binding.
    ///
    /// IPv6 sockets are IPv6-only. A port of zero selects an ephemeral port.
    pub fn bind_with_reuse(addr: Addr, handle: &Handle, reuse_addr: bool) -> Result<Self> {
        Self::from_socket(DatagramSocket::bind_with_reuse(addr, reuse_addr)?, handle)
    }

    /// Adopt an existing non-blocking datagram socket.
    pub fn from_socket(socket: DatagramSocket, handle: &Handle) -> Result<Self> {
        let registration = handle.register(socket.raw(), Interest::BOTH)?;
        Ok(Self {
            registration,
            inner: socket,
        })
    }

    /// Select a peer, returning the owned socket once the operation completes.
    ///
    /// UDP has no handshake. The kernel filters incoming datagrams by the
    /// selected peer. The peer must have the same address family as the socket.
    ///
    /// This future owns the socket: cancelling an in-flight connect closes
    /// it, rather than exposing a socket whose peer may have changed.
    pub async fn connect(self, addr: Addr) -> Result<Self> {
        match self.inner.connect(addr) {
            Ok(()) => {
                self.registration.mark_writable();
                return Ok(self);
            }
            Err(error)
                if error.0 == libc::EINPROGRESS
                    || error.0 == libc::EALREADY
                    || error.interrupted() => {}
            Err(error) if error.0 == libc::EISCONN => {
                if self.inner.peer_addr()? == addr {
                    return Ok(self);
                }
                return Err(error);
            }
            Err(error) => return Err(error),
        }

        core::future::poll_fn(|cx| {
            if !self.registration.take_writable_or_park(cx.waker()) {
                return Poll::Pending;
            }
            loop {
                match self.inner.connect_error() {
                    Ok(()) => break,
                    Err(error) if error.interrupted() => continue,
                    Err(error) => return Poll::Ready(Err(error)),
                }
            }
            match self.inner.peer_addr() {
                Ok(peer) if peer == addr => {
                    self.registration.mark_writable();
                    Poll::Ready(Ok(()))
                }
                Ok(_) => Poll::Ready(Err(crate::reactor::error::Errno(libc::EINVAL))),
                Err(error) if error.0 == libc::ENOTCONN || error.interrupted() => {
                    self.registration.poll_writable(cx.waker());
                    Poll::Pending
                }
                Err(error) => Poll::Ready(Err(error)),
            }
        })
        .await?;
        Ok(self)
    }

    /// This socket's bound address and actual local port.
    pub fn local_addr(&self) -> Result<Addr> {
        self.inner.local_addr()
    }

    /// The configured peer, or `ENOTCONN` when unconnected.
    pub fn peer_addr(&self) -> Result<Addr> {
        self.inner.peer_addr()
    }

    /// Receive one datagram, returning its copied length and source.
    ///
    /// Use [`Self::recv_datagram`] when truncation metadata is needed.
    pub fn recv_from<'a>(&'a self, buffer: &'a mut [u8]) -> RecvFrom<'a> {
        RecvFrom {
            socket: self,
            buffer,
            waiting: false,
        }
    }

    /// Set the kernel receive-buffer capacity in bytes.
    pub fn set_recv_buffer(&self, size: usize) -> Result<()> {
        self.inner.set_recv_buffer(size)
    }

    /// Set the kernel send-buffer capacity in bytes.
    pub fn set_send_buffer(&self, size: usize) -> Result<()> {
        self.inner.set_send_buffer(size)
    }

    /// Receive one datagram into an initialised slice.
    ///
    /// An empty buffer still consumes one datagram and reports truncation
    /// when that datagram is nonempty. Zero bytes never mean end of stream.
    pub fn recv_datagram<'a>(&'a self, buffer: &'a mut [u8]) -> RecvDatagram<'a> {
        RecvDatagram {
            socket: self,
            buffer,
            waiting: false,
        }
    }

    /// Append one datagram directly into a `BytesMut`'s spare capacity.
    ///
    /// No staging copy or initialisation of the spare capacity is needed.
    /// The returned length counts only the appended bytes. The caller can
    /// freeze the buffer to share immutable packet ownership without copying.
    ///
    /// An empty spare region returns `ENOBUFS` without consuming a datagram.
    pub fn recv_from_buf<'a>(&'a self, buffer: &'a mut bytes::BytesMut) -> RecvFromBuf<'a> {
        RecvFromBuf {
            socket: self,
            buffer,
            waiting: false,
        }
    }

    /// Send one datagram without copying or retaining the supplied bytes.
    ///
    /// This also sends empty datagrams. An oversized packet is an error;
    /// no suffix is retried as a separate datagram.
    pub fn send_to<'a>(&'a self, buffer: &'a [u8], destination: Addr) -> SendTo<'a> {
        SendTo {
            socket: self,
            buffer,
            destination: Some(destination),
            waiting: false,
        }
    }

    /// Send one datagram to the configured peer.
    ///
    /// Returns the kernel's error if no peer has been configured.
    pub fn send<'a>(&'a self, buffer: &'a [u8]) -> SendTo<'a> {
        SendTo {
            socket: self,
            buffer,
            destination: None,
            waiting: false,
        }
    }

    /// Poll receiving one datagram, returning its copied length and source.
    pub fn poll_recv_from(
        &self,
        cx: &mut Context<'_>,
        buffer: &mut [u8],
    ) -> Poll<Result<(usize, Addr)>> {
        self.poll_recv_datagram(cx, buffer)
            .map(|result| result.map(|packet| (packet.len, packet.source)))
    }

    /// Poll for one datagram, parking the reader only after `WouldBlock`.
    pub fn poll_recv_datagram(
        &self,
        cx: &mut Context<'_>,
        buffer: &mut [u8],
    ) -> Poll<Result<Datagram>> {
        // SAFETY: the slice is live and writable for its whole length.
        unsafe { self.poll_recv_into(cx, buffer.as_mut_ptr(), buffer.len()) }
    }

    /// Poll a receive directly into uninitialised spare buffer capacity.
    pub fn poll_recv_from_buf(
        &self,
        cx: &mut Context<'_>,
        buffer: &mut bytes::BytesMut,
    ) -> Poll<Result<Datagram>> {
        let filled = buffer.len();
        let spare = buffer.spare_capacity_mut();
        if spare.is_empty() {
            self.cancel_recv();
            return Poll::Ready(Err(crate::reactor::error::Errno(libc::ENOBUFS)));
        }
        let pointer = spare.as_mut_ptr().cast::<u8>();
        let capacity = spare.len();
        // SAFETY: the spare region is allocated, writable and disjoint from
        // the buffer's initialised prefix. The syscall reports copied bytes.
        let result = unsafe { self.poll_recv_into(cx, pointer, capacity) };
        if let Poll::Ready(Ok(datagram)) = &result {
            // SAFETY: precisely datagram.len bytes in the spare region were
            // initialised, and the low-level socket clamps that to capacity.
            unsafe { buffer.set_len(filled + datagram.len) };
        }
        result
    }

    unsafe fn poll_recv_into(
        &self,
        cx: &mut Context<'_>,
        pointer: *mut u8,
        len: usize,
    ) -> Poll<Result<Datagram>> {
        if !self.registration.take_readable_or_park(cx.waker()) {
            return Poll::Pending;
        }
        loop {
            // SAFETY: the caller guarantees the writable buffer region.
            match unsafe { self.inner.recv_datagram(pointer, len) } {
                Ok(datagram) => {
                    // A single datagram never proves the receive queue empty,
                    // regardless of its size or whether it was truncated.
                    self.registration.mark_readable();
                    self.cancel_recv();
                    return Poll::Ready(Ok(datagram));
                }
                Err(error) if error.would_block() => {
                    self.registration.poll_readable(cx.waker());
                    return Poll::Pending;
                }
                Err(error) if error.interrupted() => continue,
                Err(error) => {
                    // An asynchronous UDP error need not drain queued data.
                    self.registration.mark_readable();
                    self.cancel_recv();
                    return Poll::Ready(Err(error));
                }
            }
        }
    }

    /// Poll sending one datagram to an explicit destination.
    pub fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buffer: &[u8],
        destination: Addr,
    ) -> Poll<Result<usize>> {
        self.poll_send_datagram(cx, buffer, Some(destination))
    }

    /// Poll sending one datagram to the configured peer.
    pub fn poll_send(&self, cx: &mut Context<'_>, buffer: &[u8]) -> Poll<Result<usize>> {
        self.poll_send_datagram(cx, buffer, None)
    }

    fn poll_send_datagram(
        &self,
        cx: &mut Context<'_>,
        buffer: &[u8],
        destination: Option<Addr>,
    ) -> Poll<Result<usize>> {
        if !self.registration.take_writable_or_park(cx.waker()) {
            return Poll::Pending;
        }
        loop {
            let result = match destination {
                Some(destination) => self.inner.send_to(buffer, destination),
                None => self.inner.send(buffer),
            };
            match result {
                Ok(sent) => {
                    self.registration.mark_writable();
                    self.cancel_send();
                    return Poll::Ready(Ok(sent));
                }
                Err(error) if error.would_block() => {
                    self.registration.poll_writable(cx.waker());
                    return Poll::Pending;
                }
                Err(error) if error.interrupted() => continue,
                Err(error) => {
                    // An invalid destination or oversized packet does not
                    // establish that the socket's send buffer is full.
                    self.registration.mark_writable();
                    self.cancel_send();
                    return Poll::Ready(Err(error));
                }
            }
        }
    }

    /// Release the waiter for an abandoned direct receive poll.
    ///
    /// Cached readiness is preserved. Receive futures call this on drop.
    pub fn cancel_recv(&self) {
        self.registration.cancel_read_wait();
    }

    /// Release the waiter for an abandoned direct send poll.
    ///
    /// Cached readiness is preserved. Send futures call this on drop.
    pub fn cancel_send(&self) {
        self.registration.cancel_write_wait();
    }
}

/// The cancellation-safe future returned by [`UdpSocket::recv_from`].
#[derive(Debug)]
pub struct RecvFrom<'a> {
    socket: &'a UdpSocket,
    buffer: &'a mut [u8],
    waiting: bool,
}

impl core::future::Future for RecvFrom<'_> {
    type Output = Result<(usize, Addr)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let result = this.socket.poll_recv_from(cx, this.buffer);
        this.waiting = result.is_pending();
        result
    }
}

impl Drop for RecvFrom<'_> {
    fn drop(&mut self) {
        if self.waiting {
            self.socket.cancel_recv();
        }
    }
}

/// The cancellation-safe future returned by [`UdpSocket::recv_datagram`].
#[derive(Debug)]
pub struct RecvDatagram<'a> {
    socket: &'a UdpSocket,
    buffer: &'a mut [u8],
    waiting: bool,
}

impl core::future::Future for RecvDatagram<'_> {
    type Output = Result<Datagram>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let result = this.socket.poll_recv_datagram(cx, this.buffer);
        this.waiting = result.is_pending();
        result
    }
}

impl Drop for RecvDatagram<'_> {
    fn drop(&mut self) {
        if self.waiting {
            self.socket.cancel_recv();
        }
    }
}

/// The cancellation-safe future returned by [`UdpSocket::recv_from_buf`].
#[derive(Debug)]
pub struct RecvFromBuf<'a> {
    socket: &'a UdpSocket,
    buffer: &'a mut bytes::BytesMut,
    waiting: bool,
}

impl core::future::Future for RecvFromBuf<'_> {
    type Output = Result<Datagram>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let result = this.socket.poll_recv_from_buf(cx, this.buffer);
        this.waiting = result.is_pending();
        result
    }
}

impl Drop for RecvFromBuf<'_> {
    fn drop(&mut self) {
        if self.waiting {
            self.socket.cancel_recv();
        }
    }
}

/// The cancellation-safe future returned by UDP send operations.
#[derive(Debug)]
pub struct SendTo<'a> {
    socket: &'a UdpSocket,
    buffer: &'a [u8],
    destination: Option<Addr>,
    waiting: bool,
}

impl core::future::Future for SendTo<'_> {
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let result = this
            .socket
            .poll_send_datagram(cx, this.buffer, this.destination);
        this.waiting = result.is_pending();
        result
    }
}

impl Drop for SendTo<'_> {
    fn drop(&mut self) {
        if self.waiting {
            self.socket.cancel_send();
        }
    }
}

/// The byte stream trait this crate declared, with something behind it at last.
///
/// [`io::Stream`](crate::io::Stream) was written here because two crates always
/// need it and neither should depend on the other: a TLS session implements it
/// so a protocol can run over one, a protocol consumes it so it need not know
/// whether TLS is underneath. Until now nothing in this crate implemented it,
/// which left the trait describing an I/O driver that lived somewhere else.
///
/// A socket is the obvious implementation and it is here now, so a protocol
/// written against the trait can be handed a `TcpStream`, a TLS session over
/// one, or an in-memory pipe, and cannot tell which. The methods forward: the
/// inherent ones already have these signatures.
impl crate::io::Stream for TcpStream {
    async fn read(&mut self, buffer: &mut [u8]) -> Result<usize> {
        Self::read(self, buffer).await
    }

    async fn write_all(&mut self, buffer: &[u8]) -> Result<()> {
        Self::write_all(self, buffer).await
    }
}
