//// A socket that sends and receives whole STOMP frames. The client uses
//// it so it can work with frames and leave bytes and buffering to this
//// module.

import gleam/bool
import gleam/erlang/process.{type Pid, type Selector}
import gleam/result
import glomp/frame.{type Frame}
import glomp/internal/clock
import glomp/internal/socket.{type Socket}
import glomp/network.{type NetworkError}

pub opaque type Transport {
  Transport(socket: Socket, decoder: frame.Decoder, version: frame.Version)
}

/// The next buffered frame, if any.
pub type Next {
  Frame(frame: Frame, transport: Transport)
  Empty(transport: Transport)
}

/// Why `receive_frame` failed.
pub type Ended {
  TimedOut
  Closed
  Failed(error: NetworkError)
  Malformed(error: frame.DecodeError)
}

/// The result of handling a socket message.
pub type Arrival {
  /// The message is from a different socket.
  Stale
  Received(transport: Transport)
  PeerClosed
  Errored(error: NetworkError)
}

pub fn connect(
  host host: String,
  port port: Int,
  security security: socket.Security,
  limits limits: frame.Limits,
  timeout_ms timeout_ms: Int,
) -> Result(Transport, socket.ConnectError) {
  use socket <- result.map(socket.connect(host, port, security, timeout_ms))

  Transport(socket:, decoder: frame.decoder(limits), version: frame.Stomp12)
}

/// Set the STOMP version for encoding and decoding.
pub fn set_version(transport: Transport, version: frame.Version) -> Transport {
  Transport(
    ..transport,
    version:,
    decoder: frame.set_version(transport.decoder, version),
  )
}

pub fn send(transport: Transport, frame: Frame) -> Result(Nil, socket.Failure) {
  socket.send(transport.socket, frame.encode(frame, transport.version))
}

pub fn send_heart_beat(transport: Transport) -> Result(Nil, socket.Failure) {
  socket.send(transport.socket, frame.heart_beat)
}

/// Get the next frame from the buffer.
pub fn next(transport: Transport) -> Result(Next, frame.DecodeError) {
  use step <- result.map(frame.next(transport.decoder))

  case step {
    frame.Decoded(frame:, decoder:) ->
      Frame(frame, Transport(..transport, decoder:))

    frame.Incomplete(decoder:) -> Empty(Transport(..transport, decoder:))
  }
}

/// Block until a frame arrives or `deadline` passes.
pub fn receive_frame(
  transport: Transport,
  deadline: Int,
) -> Result(#(Frame, Transport), Ended) {
  use buffered <- result.try(next(transport) |> result.map_error(Malformed))

  case buffered {
    Frame(frame:, transport:) -> Ok(#(frame, transport))

    Empty(transport:) -> {
      let remaining = deadline - clock.now_ms()
      use <- bool.guard(remaining <= 0, Error(TimedOut))

      case socket.receive(transport.socket, remaining) {
        Ok(bytes) -> receive_frame(append(transport, bytes), deadline)
        Error(socket.TimedOut) -> Error(TimedOut)
        Error(socket.Closed) -> Error(Closed)
        Error(socket.Failed(error:)) -> Error(Failed(error))
      }
    }
  }
}

/// Deliver the next packet to the owner as a message.
pub fn set_active_once(transport: Transport) -> Result(Nil, socket.Failure) {
  socket.set_active_once(transport.socket)
}

/// Stop message delivery. Bytes already delivered stay in the mailbox.
pub fn set_passive(transport: Transport) -> Result(Transport, socket.Failure) {
  use Nil <- result.map(socket.set_passive(transport.socket))

  append(transport, socket.take_delivered(transport.socket))
}

/// Take bytes already delivered to the mailbox, if any.
pub fn take_delivered(transport: Transport) -> Result(Transport, Nil) {
  case socket.take_delivered(transport.socket) {
    <<>> -> Error(Nil)
    bytes -> Ok(append(transport, bytes))
  }
}

/// Handle a socket message from `select`.
pub fn arrival(transport: Transport, incoming: socket.Incoming) -> Arrival {
  use <- bool.guard(incoming.socket != transport.socket, Stale)

  case incoming.event {
    socket.Arrived(bytes:) -> Received(append(transport, bytes))
    socket.Ended -> PeerClosed
    socket.Errored(error:) -> Errored(error)
  }
}

/// Select messages from transports in active-once mode.
pub fn select(
  selector: Selector(message),
  mapping: fn(socket.Incoming) -> message,
) -> Selector(message) {
  socket.select(selector, mapping)
}

pub fn hand_over(transport: Transport, to: Pid) -> Result(Nil, socket.Failure) {
  socket.hand_over(transport.socket, to)
}

pub fn close(transport: Transport) -> Nil {
  socket.close(transport.socket)
}

fn append(transport: Transport, bytes: BitArray) -> Transport {
  Transport(..transport, decoder: frame.feed(transport.decoder, bytes))
}
