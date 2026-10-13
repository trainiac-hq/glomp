import gleam/bool
import gleam/erlang/process.{type Pid}
import gleam/list
import glomp/frame.{type Frame, Frame}

pub type Listener

pub type Socket

pub type Broker {
  Broker(listener: Listener, port: Int)
}

pub type TlsBroker {
  TlsBroker(listener: Listener, port: Int, authority: String)
}

pub type Peer {
  Peer(socket: Socket, decoder: frame.Decoder)
}

pub type ReceiveError {
  TimedOut
  Closed
}

@external(erlang, "broker_ffi", "listen")
fn listen() -> #(Listener, Int)

@external(erlang, "broker_ffi", "listen_tls")
fn listen_tls() -> #(Listener, Int, String)

@external(erlang, "broker_ffi", "accept")
fn accept_socket(listener: Listener, timeout: Int) -> Result(Socket, Nil)

@external(erlang, "broker_ffi", "recv")
pub fn receive_bytes(
  socket: Socket,
  timeout: Int,
) -> Result(BitArray, ReceiveError)

@external(erlang, "broker_ffi", "send")
pub fn send_bytes(socket: Socket, bytes: BitArray) -> Nil

@external(erlang, "broker_ffi", "close")
pub fn close(socket: Socket) -> Nil

@external(erlang, "broker_ffi", "stop")
pub fn stop(listener: Listener) -> Nil

@external(erlang, "broker_ffi", "free_port")
pub fn free_port() -> Int

pub fn start() -> Broker {
  let #(listener, port) = listen()

  Broker(listener:, port:)
}

pub fn start_tls() -> TlsBroker {
  let #(listener, port, authority) = listen_tls()

  TlsBroker(listener:, port:, authority:)
}

pub fn accept(listener: Listener) -> Peer {
  let assert Ok(socket) = accept_socket(listener, 5000)
    as "no client connected to the test broker"

  Peer(socket:, decoder: frame.decoder(frame.default_limits))
}

pub fn nobody_connects(listener: Listener, within timeout: Int) -> Bool {
  case accept_socket(listener, timeout) {
    Ok(socket) -> {
      close(socket)
      False
    }

    Error(Nil) -> True
  }
}

pub fn receive(peer: Peer) -> #(Frame, Peer) {
  let assert Ok(step) = frame.next(peer.decoder)

  case step {
    frame.Decoded(frame: received, decoder:) -> #(
      received,
      Peer(..peer, decoder:),
    )

    frame.Incomplete(decoder:) -> {
      let assert Ok(bytes) = receive_bytes(peer.socket, 5000)
        as "the client sent nothing to the test broker"

      receive(Peer(..peer, decoder: frame.feed(decoder, bytes)))
    }
  }
}

pub fn send(peer: Peer, frame: Frame) -> Nil {
  send_bytes(peer.socket, frame.encode(frame, frame.Stomp12))
}

pub fn accept_stomp(listener: Listener) -> #(Frame, Peer) {
  accept_with(listener, [#("version", "1.2"), #("heart-beat", "0,0")])
}

pub fn accept_with(
  listener: Listener,
  headers: List(#(String, String)),
) -> #(Frame, Peer) {
  let #(connect, peer) = receive(accept(listener))

  send(peer, Frame(frame.Connected, headers, <<>>))
  #(connect, peer)
}

pub fn receive_subscribe(peer: Peer) -> #(Frame, Peer) {
  let #(subscribe, peer) = receive(peer)
  let assert frame.Subscribe = subscribe.command

  send_receipt(peer, subscribe)
  #(subscribe, peer)
}

pub fn send_receipt(peer: Peer, asked: Frame) -> Nil {
  let assert Ok(receipt) = frame.header(asked, "receipt")

  send(peer, Frame(frame.Receipt, [#("receipt-id", receipt)], <<>>))
}

pub fn send_message(
  peer: Peer,
  headers: List(#(String, String)),
  body: BitArray,
) -> Nil {
  send(peer, Frame(frame.Message, headers, body))
}

pub fn closes(peer: Peer, within timeout: Int) -> Bool {
  case receive_bytes(peer.socket, timeout) {
    Ok(bytes) ->
      closes(Peer(..peer, decoder: frame.feed(peer.decoder, bytes)), timeout)

    Error(Closed) -> True
    Error(TimedOut) -> False
  }
}

pub fn headers_but_receipt(frame: Frame) -> List(#(String, String)) {
  list.filter(frame.headers, fn(header) { header.0 != "receipt" })
}

@external(erlang, "broker_ffi", "only_child")
pub fn only_child(supervisor: Pid) -> Pid

@external(erlang, "broker_ffi", "try_send")
pub fn try_send_bytes(socket: Socket, bytes: BitArray, timeout: Int) -> Bool

pub fn receive_all_bytes(
  socket: Socket,
  quiet: Int,
  so_far: BitArray,
) -> BitArray {
  case receive_bytes(socket, quiet) {
    Ok(bytes) -> receive_all_bytes(socket, quiet, <<so_far:bits, bytes:bits>>)
    Error(TimedOut) | Error(Closed) -> so_far
  }
}

@external(erlang, "broker_ffi", "reset")
pub fn reset(socket: Socket) -> Nil

/// The processes linked to `process`, without `except`.
@external(erlang, "broker_ffi", "linked_processes")
pub fn linked_processes(process: Pid, except except: Pid) -> List(Pid)

/// Whether every process exits within `timeout`.
pub fn all_exit(processes: List(Pid), within timeout: Int) -> Bool {
  case list.filter(processes, process.is_alive) {
    [] -> True

    [first, ..rest] -> {
      use <- bool.guard(timeout <= 0, False)

      process.sleep(10)
      all_exit([first, ..rest], within: timeout - 10)
    }
  }
}
