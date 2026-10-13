//// Shared helpers for the client tests.

import broker.{type Peer}
import gleam/erlang/process.{type Subject}
import glomp
import glomp/frame

/// A test client: no heart-beats, 20ms reconnect, events forwarded.
pub fn test_client(port: Int, events: Subject(glomp.Event)) -> glomp.Client {
  glomp.new(host: "127.0.0.1", port:)
  |> glomp.heart_beat(send_ms: 0, receive_ms: 0)
  |> glomp.reconnect(first_ms: 20, longest_ms: 80)
  |> glomp.on_event(process.send(events, _))
}

pub fn start(client: glomp.Client) -> glomp.Client {
  let assert Ok(started) = glomp.start(client)

  started.data
}

pub fn next(subject: Subject(a)) -> a {
  let assert Ok(value) = process.receive(subject, 5000)
    as "nothing arrived in five seconds"

  value
}

pub fn receives_nothing(subject: Subject(a), within timeout: Int) -> Bool {
  process.receive(subject, timeout) == Error(Nil)
}

pub fn connected(events: Subject(glomp.Event)) -> glomp.Session {
  let assert glomp.Connected(session) = next(events)

  session
}

/// Start a client and complete the handshake.
pub fn connect(
  client: glomp.Client,
  server: broker.Broker,
  events: Subject(glomp.Event),
) -> #(glomp.Client, Peer) {
  let client = start(client)
  let #(connect, peer) = broker.accept_stomp(server.listener)
  assert connect.command == frame.Connect
  assert connected(events).version == frame.Stomp12

  #(client, peer)
}

/// Run `call` in another process.
pub fn async(call: fn() -> a) -> Subject(a) {
  let done = process.new_subject()

  process.spawn(fn() { process.send(done, call()) })
  done
}

/// Stop the client and send the DISCONNECT receipt.
pub fn stop(client: glomp.Client, peer: Peer) -> glomp.Stopped {
  let stopped = async(fn() { glomp.stop(client) })
  let #(disconnect, peer) = broker.receive(peer)
  assert disconnect.command == frame.Disconnect

  broker.send_receipt(peer, disconnect)

  let stopped = next(stopped)
  assert broker.closes(peer, within: 1000)

  stopped
}

/// The headers of a MESSAGE for subscription `subscription`.
pub fn to(subscription: String, id: String) -> List(#(String, String)) {
  [
    #("subscription", subscription),
    #("message-id", id),
    #("destination", "/d/" <> subscription),
  ]
}
