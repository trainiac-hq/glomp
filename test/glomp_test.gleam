import broker.{type Peer}
import gleam/erlang/atom
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/option.{None, Some}
import gleam/otp/actor
import gleam/otp/static_supervisor
import gleam/otp/supervision
import gleeunit
import glomp
import glomp/frame.{Frame}
import glomp/network

pub fn main() -> Nil {
  gleeunit.main()
}

/// A test client: no heart-beats, 20ms reconnect, events forwarded.
fn test_client(port: Int, events: Subject(glomp.Event)) -> glomp.Client {
  glomp.new(host: "127.0.0.1", port:)
  |> glomp.heart_beat(send_ms: 0, receive_ms: 0)
  |> glomp.reconnect(first_ms: 20, longest_ms: 80)
  |> glomp.on_event(process.send(events, _))
}

fn start(client: glomp.Client) -> glomp.Client {
  let assert Ok(started) = glomp.start(client)

  started.data
}

fn connected(events: Subject(glomp.Event)) -> glomp.Session {
  let assert glomp.Connected(session) = next(events)

  session
}

/// Start a client and complete the handshake.
fn connect(
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
fn async(call: fn() -> a) -> Subject(a) {
  let done = process.new_subject()

  process.spawn(fn() { process.send(done, call()) })
  done
}

fn next(subject: Subject(a)) -> a {
  let assert Ok(value) = process.receive(subject, 5000)
    as "nothing arrived in five seconds"

  value
}

fn receives_nothing(subject: Subject(a), within timeout: Int) -> Bool {
  process.receive(subject, timeout) == Error(Nil)
}

/// Stop the client and send the DISCONNECT receipt.
fn stop(client: glomp.Client, peer: Peer) -> glomp.Stopped {
  let stopped = process.new_subject()

  process.spawn(fn() { process.send(stopped, glomp.stop(client)) })

  let #(disconnect, peer) = broker.receive(peer)
  assert disconnect.command == frame.Disconnect

  broker.send_receipt(peer, disconnect)

  let stopped = next(stopped)
  assert broker.closes(peer, within: 1000)

  stopped
}

pub fn connects_test() {
  let server = broker.start()
  let events = process.new_subject()

  let client =
    test_client(server.port, events)
    |> glomp.credentials(login: "user", passcode: "secret")
    |> glomp.virtual_host("/vhost")
    |> glomp.client_id("consumer-1")
    |> glomp.connect_header(name: "x-extra", value: "yes")
    |> glomp.heart_beat(send_ms: 4000, receive_ms: 5000)
    |> start

  assert glomp.await_connected(client, within: 0) == Error(Nil)

  let #(connect, peer) =
    broker.accept_with(server.listener, [
      #("version", "1.2"),
      #("server", "test/1.0"),
      #("session", "session-9"),
      #("heart-beat", "6000,1000"),
    ])

  assert connect
    == Frame(
      frame.Connect,
      [
        #("accept-version", "1.1,1.2"),
        #("host", "/vhost"),
        #("login", "user"),
        #("passcode", "secret"),
        #("heart-beat", "4000,5000"),
        #("client-id", "consumer-1"),
        #("x-extra", "yes"),
      ],
      <<>>,
    )

  let session =
    glomp.Session(
      version: frame.Stomp12,
      server: Some("test/1.0"),
      id: Some("session-9"),
      // Each direction uses the larger number.
      heart_beat_send_ms: 4000,
      heart_beat_receive_ms: 6000,
      headers: [
        #("version", "1.2"),
        #("server", "test/1.0"),
        #("session", "session-9"),
        #("heart-beat", "6000,1000"),
      ],
    )

  assert next(events) == glomp.Connected(session)
  assert glomp.await_connected(client, within: 1000) == Ok(Nil)
  assert stop(client, peer) == glomp.Graceful
}

pub fn defaults_test() {
  let server = broker.start()

  let events = process.new_subject()

  let client =
    glomp.new(host: "localhost", port: server.port)
    |> glomp.on_event(process.send(events, _))
    |> start

  let #(connect, peer) = broker.accept_stomp(server.listener)

  assert connect.headers
    == [
      #("accept-version", "1.1,1.2"),
      #("host", "localhost"),
      #("heart-beat", "30000,30000"),
    ]

  let session = connected(events)
  assert session.server == None
  assert session.id == None
  // The broker sent 0,0: no heart-beats.
  assert session.heart_beat_send_ms == 0
  assert session.heart_beat_receive_ms == 0
  assert stop(client, peer) == glomp.Graceful
}

/// `await_connected` called before connecting returns after the connect.
pub fn await_connected_waits_test() {
  let server = broker.start()
  let client = test_client(server.port, process.new_subject()) |> start
  let waited = process.new_subject()

  process.spawn(fn() {
    process.send(waited, glomp.await_connected(client, within: 5000))
  })

  assert receives_nothing(waited, within: 50)

  let #(connect, peer) = broker.accept_stomp(server.listener)
  assert connect.command == frame.Connect

  assert next(waited) == Ok(Nil)
  assert stop(client, peer) == glomp.Graceful
}

pub fn subscribes_and_delivers_test() {
  let server = broker.start()
  let events = process.new_subject()
  let messages = process.new_subject()
  let deferred = process.new_subject()

  let client =
    test_client(server.port, events)
    |> glomp.with_subscription(
      glomp.subscription(
        id: "a",
        destination: "/topic/a",
        handler: process.send(messages, _),
      )
      |> glomp.subscription_header(name: "selector", value: "x = 1"),
    )
    |> glomp.with_subscription(
      glomp.acknowledged_subscription(
        id: "b",
        destination: "/queue/b",
        mode: glomp.Individual,
        handler: fn(message, delivery) {
          process.send(messages, message)
          process.send(deferred, delivery)
          glomp.Defer
        },
      ),
    )
    |> start

  let #(connect, peer) = broker.accept_stomp(server.listener)
  assert connect.command == frame.Connect
  assert connected(events).version == frame.Stomp12

  let #(first, peer) = broker.receive(peer)
  assert first.command == frame.Subscribe

  assert broker.headers_but_receipt(first)
    == [
      #("id", "a"),
      #("destination", "/topic/a"),
      #("ack", "auto"),
      #("selector", "x = 1"),
    ]

  let #(second, peer) = broker.receive(peer)

  assert broker.headers_but_receipt(second)
    == [
      #("id", "b"),
      #("destination", "/queue/b"),
      #("ack", "client-individual"),
    ]

  // `Subscribed` is emitted only after the receipt.
  assert receives_nothing(events, within: 30)

  broker.send_receipt(peer, second)

  assert next(events) == glomp.Subscribed("b")

  broker.send_receipt(peer, first)

  assert next(events) == glomp.Subscribed("a")

  let headers = [
    #("subscription", "a"),
    #("message-id", "m-1"),
    #("destination", "/topic/a/deeper"),
    #("content-type", "text/plain"),
  ]

  broker.send_message(peer, headers, <<"hello":utf8>>)

  let message = next(messages)

  assert message
    == glomp.Message(
      destination: "/topic/a/deeper",
      id: "m-1",
      subscription: "a",
      headers:,
      body: <<"hello":utf8>>,
    )

  assert glomp.header(message, "content-type") == Ok("text/plain")
  assert glomp.header(message, "missing") == Error(Nil)
  assert stop(client, peer) == glomp.Graceful
}

pub fn sends_test() {
  let server = broker.start()
  let events = process.new_subject()
  let #(client, peer) =
    test_client(server.port, events) |> connect(server, events)

  assert glomp.send(
      client,
      glomp.message(to: "/queue/a", body: <<"hi":utf8>>)
        |> glomp.content_type("text/plain")
        |> glomp.message_header(name: "persistent", value: "true"),
    )
    == Ok(Nil)

  let #(sent, peer) = broker.receive(peer)

  assert sent
    == Frame(
      frame.Send,
      [
        #("destination", "/queue/a"),
        #("content-type", "text/plain"),
        #("content-length", "2"),
        #("persistent", "true"),
      ],
      <<"hi":utf8>>,
    )

  assert glomp.send(
      client,
      glomp.message(to: "/queue/a", body: <<"text":utf8>>)
        |> glomp.without_content_length,
    )
    == Ok(Nil)

  let #(sent, peer) = broker.receive(peer)

  assert sent
    == Frame(frame.Send, [#("destination", "/queue/a")], <<"text":utf8>>)

  // A body with a NUL always gets its length.
  assert glomp.send(
      client,
      glomp.message(to: "/queue/a", body: <<1, 0, 2>>)
        |> glomp.without_content_length,
    )
    == Ok(Nil)

  let #(sent, peer) = broker.receive(peer)

  assert sent
    == Frame(
      frame.Send,
      [#("destination", "/queue/a"), #("content-length", "3")],
      <<1, 0, 2>>,
    )

  assert stop(client, peer) == glomp.Graceful
}

pub fn send_confirmed_test() {
  let server = broker.start()
  let events = process.new_subject()
  let #(client, peer) =
    test_client(server.port, events) |> connect(server, events)
  let outbound = glomp.message(to: "/queue/a", body: <<"hi":utf8>>)

  let sent =
    async(fn() { glomp.send_confirmed(client, outbound, within: 5000) })

  let #(send, peer) = broker.receive(peer)

  assert broker.headers_but_receipt(send)
    == [#("destination", "/queue/a"), #("content-length", "2")]

  // No reply until the receipt arrives.
  assert receives_nothing(sent, within: 30)

  broker.send_receipt(peer, send)

  assert next(sent) == Ok(Nil)

  // No receipt arrives in time.
  assert glomp.send_confirmed(client, outbound, within: 30)
    == Error(glomp.NoReceipt(30))

  let #(late, peer) = broker.receive(peer)
  let assert Ok(receipt) = frame.header(late, "receipt")

  broker.send_receipt(peer, late)

  assert next(events)
    == glomp.Dropped(
      Frame(frame.Receipt, [#("receipt-id", receipt)], <<>>),
      glomp.UnknownReceipt(receipt),
    )

  // The connection closes before the receipt.
  let sent =
    async(fn() { glomp.send_confirmed(client, outbound, within: 5000) })

  let #(unanswered, peer) = broker.receive(peer)
  assert unanswered.command == frame.Send

  broker.close(peer.socket)

  assert next(sent) == Error(glomp.ConnectionLost(glomp.Closed))
  assert next(events) == glomp.Disconnected(glomp.Closed, 20)
  assert glomp.stop(client) == glomp.Idle
}

/// With no broker the wait doubles and requests fail.
pub fn backs_off_test() {
  let events = process.new_subject()
  let client = test_client(broker.free_port(), events) |> start

  let waits =
    list.map([1, 2, 3, 4, 5], fn(attempt) {
      let assert glomp.Disconnected(glomp.Unreachable(ipv4:, ipv6:), wait) =
        next(events)

      assert ipv4 == network.ConnectionRefused
        as { "attempt " <> int.to_string(attempt) }

      assert ipv6 != None

      wait
    })

  assert waits == [20, 40, 80, 80, 80]

  let outbound = glomp.message(to: "/queue/a", body: <<>>)
  assert glomp.send(client, outbound) == Error(glomp.NotConnected)

  assert glomp.send_confirmed(client, outbound, within: 10)
    == Error(glomp.NotConnected)

  let assert Error(glomp.NotConnected) = glomp.begin(client)
  assert glomp.await_connected(client, within: 10) == Error(Nil)
  assert glomp.stop(client) == glomp.Idle
}

/// Verdicts send ACK or NACK. Deferred ones are sent later.
pub fn acknowledges_test() {
  let server = broker.start()
  let events = process.new_subject()
  let deferred = process.new_subject()

  let #(client, peer) =
    test_client(server.port, events)
    |> glomp.with_subscription(
      glomp.acknowledged_subscription(
        id: "q",
        destination: "/queue/q",
        mode: glomp.Cumulative,
        handler: fn(message, delivery) {
          case message.body {
            <<"ack":utf8>> -> glomp.Ack
            <<"nack":utf8>> -> glomp.Nack

            other -> {
              process.send(deferred, #(other, delivery))
              glomp.Defer
            }
          }
        },
      ),
    )
    |> connect(server, events)

  let #(subscribe, peer) = broker.receive_subscribe(peer)

  assert broker.headers_but_receipt(subscribe)
    == [#("id", "q"), #("destination", "/queue/q"), #("ack", "client")]

  assert next(events) == glomp.Subscribed("q")

  let send = fn(ack, body) {
    broker.send_message(
      peer,
      [
        #("subscription", "q"),
        #("message-id", "m-" <> ack),
        #("destination", "/queue/q"),
        #("ack", ack),
      ],
      body,
    )
  }

  send("a1", <<"ack":utf8>>)

  let #(settled, peer) = broker.receive(peer)
  assert settled == Frame(frame.Ack, [#("id", "a1")], <<>>)

  send("a2", <<"nack":utf8>>)

  let #(settled, peer) = broker.receive(peer)
  assert settled == Frame(frame.Nack, [#("id", "a2")], <<>>)

  send("a3", <<"later":utf8>>)

  let #(body, delivery) = next(deferred)
  assert body == <<"later":utf8>>
  assert glomp.ack(client, delivery) == Ok(Nil)

  let #(settled, peer) = broker.receive(peer)
  assert settled == Frame(frame.Ack, [#("id", "a3")], <<>>)

  send("a4", <<"later":utf8>>)

  let #(body, delivery) = next(deferred)
  assert body == <<"later":utf8>>
  assert glomp.nack(client, delivery) == Ok(Nil)

  let #(settled, peer) = broker.receive(peer)
  assert settled == Frame(frame.Nack, [#("id", "a4")], <<>>)

  // Without an `ack` header the message is dropped.
  let unsettleable = [
    #("subscription", "q"),
    #("message-id", "m-5"),
    #("destination", "/queue/q"),
  ]

  broker.send_message(peer, unsettleable, <<"ack":utf8>>)

  assert next(events)
    == glomp.Dropped(
      Frame(frame.Message, unsettleable, <<"ack":utf8>>),
      glomp.MissingHeader("ack"),
    )

  // A delivery from a closed connection returns `Expired`.
  send("a6", <<"later":utf8>>)

  let #(body, expired) = next(deferred)
  assert body == <<"later":utf8>>

  broker.close(peer.socket)

  assert next(events) == glomp.Disconnected(glomp.Closed, 20)
  assert glomp.ack(client, expired) == Error(glomp.Expired)

  let #(connect, peer) = broker.accept_stomp(server.listener)
  assert connect.command == frame.Connect
  assert connected(events).version == frame.Stomp12
  assert glomp.ack(client, expired) == Error(glomp.Expired)
  assert glomp.nack(client, expired) == Error(glomp.Expired)

  let #(resubscribe, peer) = broker.receive(peer)
  assert resubscribe.command == frame.Subscribe
  assert stop(client, peer) == glomp.Graceful
}

/// STOMP 1.1 acknowledges by message id and subscription.
pub fn acknowledges_on_1_1_test() {
  let server = broker.start()
  let events = process.new_subject()
  let deliveries = process.new_subject()

  let client =
    test_client(server.port, events)
    |> glomp.with_subscription(
      glomp.acknowledged_subscription(
        id: "q",
        destination: "/queue/q",
        mode: glomp.Individual,
        handler: fn(message, delivery) {
          process.send(deliveries, delivery)

          case message.body == <<"ack":utf8>> {
            True -> glomp.Ack
            False -> glomp.Nack
          }
        },
      ),
    )
    |> start

  let #(connect, peer) =
    broker.accept_with(server.listener, [#("version", "1.1")])

  assert connect.command == frame.Connect
  assert connected(events).version == frame.Stomp11

  let #(subscribe, peer) = broker.receive(peer)
  assert subscribe.command == frame.Subscribe

  broker.send_message(
    peer,
    [
      #("subscription", "q"),
      #("message-id", "m-1"),
      #("destination", "/queue/q"),
    ],
    <<"ack":utf8>>,
  )

  let #(settled, peer) = broker.receive(peer)

  assert settled
    == Frame(frame.Ack, [#("message-id", "m-1"), #("subscription", "q")], <<>>)

  assert stop(client, peer) == glomp.Graceful
}

pub fn transactions_test() {
  let server = broker.start()
  let events = process.new_subject()
  let deferred = process.new_subject()

  let #(client, peer) =
    test_client(server.port, events)
    |> glomp.with_subscription(
      glomp.acknowledged_subscription(
        id: "q",
        destination: "/queue/q",
        mode: glomp.Individual,
        handler: fn(message, delivery) {
          process.send(deferred, #(message.id, delivery))
          glomp.Defer
        },
      ),
    )
    |> connect(server, events)

  let #(subscribe, peer) = broker.receive(peer)
  assert subscribe.command == frame.Subscribe

  let assert Ok(transaction) = glomp.begin(client)
  let #(begin, peer) = broker.receive(peer)
  let assert Frame(frame.Begin, [#("transaction", id)], <<>>) = begin

  assert glomp.send(
      client,
      glomp.message(to: "/queue/a", body: <<>>)
        |> glomp.in_transaction(transaction),
    )
    == Ok(Nil)

  let #(sent, peer) = broker.receive(peer)

  assert sent.headers
    == [
      #("destination", "/queue/a"),
      #("content-length", "0"),
      #("transaction", id),
    ]

  let message = fn(ack) {
    broker.send_message(
      peer,
      [
        #("subscription", "q"),
        #("message-id", ack),
        #("destination", "/queue/q"),
        #("ack", ack),
      ],
      <<>>,
    )
  }

  message("one")

  let #(message_id, delivery) = next(deferred)
  assert message_id == "one"
  assert glomp.ack_in(client, delivery, transaction) == Ok(Nil)

  let #(settled, peer) = broker.receive(peer)

  assert settled
    == Frame(frame.Ack, [#("id", "one"), #("transaction", id)], <<>>)

  message("two")

  let #(message_id, delivery) = next(deferred)
  assert message_id == "two"
  assert glomp.nack_in(client, delivery, transaction) == Ok(Nil)

  let #(settled, peer) = broker.receive(peer)

  assert settled
    == Frame(frame.Nack, [#("id", "two"), #("transaction", id)], <<>>)

  let committed =
    async(fn() { glomp.commit(client, transaction, within: 5000) })

  let #(commit, peer) = broker.receive(peer)
  assert commit.command == frame.Commit
  assert broker.headers_but_receipt(commit) == [#("transaction", id)]
  assert receives_nothing(committed, within: 30)

  broker.send_receipt(peer, commit)

  assert next(committed) == Ok(Nil)

  let assert Ok(second) = glomp.begin(client)
  let #(begin, peer) = broker.receive(peer)
  let assert Frame(frame.Begin, [#("transaction", second_id)], <<>>) = begin
  assert second_id != id
  assert glomp.abort(client, second) == Ok(Nil)

  let #(abort, peer) = broker.receive(peer)
  assert abort == Frame(frame.Abort, [#("transaction", second_id)], <<>>)

  // A transaction from a closed connection returns `Expired`.
  let assert Ok(doomed) = glomp.begin(client)
  let #(begin, peer) = broker.receive(peer)
  assert begin.command == frame.Begin

  broker.close(peer.socket)

  assert next(events) == glomp.Disconnected(glomp.Closed, 20)

  let in_doomed =
    glomp.message(to: "/queue/a", body: <<>>) |> glomp.in_transaction(doomed)

  assert glomp.send(client, in_doomed) == Error(glomp.Expired)
  assert glomp.commit(client, doomed, within: 10) == Error(glomp.Expired)

  let #(connect, peer) = broker.accept_stomp(server.listener)
  assert connect.command == frame.Connect
  assert connected(events).version == frame.Stomp12
  assert glomp.send(client, in_doomed) == Error(glomp.Expired)
  assert glomp.commit(client, doomed, within: 10) == Error(glomp.Expired)
  assert glomp.abort(client, doomed) == Error(glomp.Expired)

  let #(resubscribe, peer) = broker.receive(peer)
  assert resubscribe.command == frame.Subscribe
  assert stop(client, peer) == glomp.Graceful
}

/// After a close the client reconnects and resubscribes.
pub fn reconnects_and_resubscribes_test() {
  let server = broker.start()
  let events = process.new_subject()
  let messages = process.new_subject()

  let #(client, peer) =
    test_client(server.port, events)
    |> glomp.with_subscription(
      glomp.subscription(
        id: "a",
        destination: "/topic/a",
        handler: process.send(messages, _),
      ),
    )
    |> connect(server, events)

  let #(first, peer) = broker.receive_subscribe(peer)
  assert next(events) == glomp.Subscribed("a")

  broker.close(peer.socket)

  assert next(events) == glomp.Disconnected(glomp.Closed, 20)

  let #(connect, peer) = broker.accept_stomp(server.listener)
  assert connect.command == frame.Connect
  assert connected(events).version == frame.Stomp12

  let #(second, peer) = broker.receive_subscribe(peer)
  assert broker.headers_but_receipt(second) == broker.headers_but_receipt(first)
  assert next(events) == glomp.Subscribed("a")

  broker.send_message(
    peer,
    [#("subscription", "a"), #("message-id", "1"), #("destination", "/topic/a")],
    <<"again":utf8>>,
  )

  assert next(messages).body == <<"again":utf8>>
  assert stop(client, peer) == glomp.Graceful
}

/// Each invalid reply to CONNECT and its `Disconnect` reason.
pub fn refused_connections_test() {
  let server = broker.start()
  let events = process.new_subject()
  let client =
    test_client(server.port, events) |> glomp.connect_timeout(100) |> start

  let attempt = fn(reply: fn(Peer) -> Nil) {
    let #(connect, peer) = broker.receive(broker.accept(server.listener))
    assert connect.command == frame.Connect

    reply(peer)

    let assert glomp.Disconnected(reason, wait) = next(events)
    // The client closes the rejected connection.
    assert broker.closes(peer, within: 1000)
    assert list.contains([20, 40, 80], wait)

    reason
  }

  let refusal =
    Frame(frame.ServerError, [#("message", "bad login")], <<"no":utf8>>)

  assert attempt(broker.send(_, refusal)) == glomp.Refused("bad login", refusal)

  let nameless = Frame(frame.ServerError, [], <<>>)
  assert attempt(broker.send(_, nameless)) == glomp.Refused("", nameless)

  let old = Frame(frame.Connected, [], <<>>)
  assert attempt(broker.send(_, old)) == glomp.UnsupportedVersion("1.0")

  let new = Frame(frame.Connected, [#("version", "2.0")], <<>>)
  assert attempt(broker.send(_, new)) == glomp.UnsupportedVersion("2.0")

  let odd = Frame(frame.Receipt, [#("receipt-id", "1")], <<>>)
  assert attempt(broker.send(_, odd)) == glomp.UnexpectedFrame(odd)

  assert attempt(fn(peer) {
      broker.send_bytes(peer.socket, <<"HTTP/1.1 400\n\n":utf8, 0>>)
    })
    == glomp.Malformed(frame.UnknownCommand("HTTP/1.1 400"))

  assert attempt(fn(peer) { broker.close(peer.socket) }) == glomp.Closed

  // The broker sends nothing.
  assert attempt(fn(peer) {
      assert broker.receive_bytes(peer.socket, 10) == Error(broker.TimedOut)
    })
    == glomp.HandshakeTimedOut(100)

  assert glomp.stop(client) == glomp.Idle
}

/// Each invalid frame on an open connection and its reason.
pub fn broken_connections_test() {
  let server = broker.start()
  let events = process.new_subject()

  let client =
    test_client(server.port, events)
    |> glomp.limits(frame.Limits(max_head_bytes: 1000, max_body_bytes: 8))
    |> start

  let attempt = fn(act: fn(Peer) -> Nil) {
    let #(connect, peer) = broker.accept_stomp(server.listener)
    assert connect.command == frame.Connect
    assert connected(events).version == frame.Stomp12

    act(peer)

    let assert glomp.Disconnected(reason, wait) = next(events)
    assert broker.closes(peer, within: 1000)
    assert list.contains([20, 40, 80], wait)

    reason
  }

  let error = Frame(frame.ServerError, [#("message", "too slow")], <<>>)
  assert attempt(broker.send(_, error)) == glomp.BrokerError("too slow", error)

  let again = Frame(frame.Connected, [#("version", "1.2")], <<>>)
  assert attempt(broker.send(_, again)) == glomp.UnexpectedFrame(again)

  let ours = Frame(frame.Send, [#("destination", "/queue/a")], <<>>)
  assert attempt(broker.send(_, ours)) == glomp.UnexpectedFrame(ours)

  assert attempt(fn(peer) {
      broker.send_bytes(peer.socket, <<"nonsense\n\n":utf8, 0>>)
    })
    == glomp.Malformed(frame.UnknownCommand("nonsense"))

  assert attempt(broker.send_message(_, [], <<"too large":utf8>>))
    == glomp.Malformed(frame.BodyTooLarge(8))

  assert glomp.stop(client) == glomp.Idle
}

/// Unusable frames emit `Dropped` and are skipped.
pub fn dropped_frames_test() {
  let server = broker.start()
  let events = process.new_subject()
  let messages = process.new_subject()

  let #(client, peer) =
    test_client(server.port, events)
    |> glomp.with_subscription(
      glomp.subscription(
        id: "a",
        destination: "/topic/a",
        handler: process.send(messages, _),
      ),
    )
    |> connect(server, events)

  let #(subscribe, peer) = broker.receive(peer)
  assert subscribe.command == frame.Subscribe

  let dropped = fn(headers, reason) {
    broker.send_message(peer, headers, <<>>)

    assert next(events)
      == glomp.Dropped(Frame(frame.Message, headers, <<>>), reason)
  }

  dropped(
    [#("message-id", "1"), #("destination", "/topic/a")],
    glomp.MissingHeader("subscription"),
  )

  dropped(
    [#("subscription", "a"), #("destination", "/topic/a")],
    glomp.MissingHeader("message-id"),
  )

  dropped(
    [#("subscription", "a"), #("message-id", "1")],
    glomp.MissingHeader("destination"),
  )

  dropped(
    [#("subscription", "b"), #("message-id", "1"), #("destination", "/topic/a")],
    glomp.UnknownSubscription("b"),
  )

  let bare = Frame(frame.Receipt, [], <<>>)

  broker.send(peer, bare)

  assert next(events) == glomp.Dropped(bare, glomp.MissingHeader("receipt-id"))

  broker.send_message(
    peer,
    [#("subscription", "a"), #("message-id", "2"), #("destination", "/topic/a")],
    <<"fine":utf8>>,
  )

  assert next(messages).body == <<"fine":utf8>>
  assert stop(client, peer) == glomp.Graceful
}

pub fn subscribes_while_running_test() {
  let server = broker.start()
  let events = process.new_subject()
  let messages = process.new_subject()

  let wanted =
    glomp.subscription(
      id: "late",
      destination: "/topic/late",
      handler: process.send(messages, _),
    )

  let #(client, peer) =
    test_client(server.port, events) |> connect(server, events)

  assert glomp.subscribe(client, wanted) == Ok(Nil)

  assert glomp.subscribe(client, wanted)
    == Error(glomp.AlreadySubscribed("late"))

  let #(subscribe, peer) = broker.receive_subscribe(peer)

  assert broker.headers_but_receipt(subscribe)
    == [#("id", "late"), #("destination", "/topic/late"), #("ack", "auto")]

  assert next(events) == glomp.Subscribed("late")

  let headers = [
    #("subscription", "late"),
    #("message-id", "1"),
    #("destination", "/topic/late"),
  ]

  broker.send_message(peer, headers, <<"one":utf8>>)

  assert next(messages).body == <<"one":utf8>>

  assert glomp.unsubscribe(client, "late") == Ok(Nil)
  assert glomp.unsubscribe(client, "late") == Error(glomp.NotSubscribed("late"))

  let #(unsubscribe, peer) = broker.receive(peer)
  assert unsubscribe == Frame(frame.Unsubscribe, [#("id", "late")], <<>>)

  // A message sent before the UNSUBSCRIBE arrived.
  broker.send_message(peer, headers, <<"two":utf8>>)

  assert next(events)
    == glomp.Dropped(
      Frame(frame.Message, headers, <<"two":utf8>>),
      glomp.UnknownSubscription("late"),
    )

  assert receives_nothing(messages, within: 30)

  // A subscription added while disconnected is sent on reconnect.
  broker.close(peer.socket)

  assert next(events) == glomp.Disconnected(glomp.Closed, 20)
  assert glomp.subscribe(client, wanted) == Ok(Nil)

  let #(connect, peer) = broker.accept_stomp(server.listener)
  assert connect.command == frame.Connect
  assert connected(events).version == frame.Stomp12

  let #(subscribe, peer) = broker.receive_subscribe(peer)
  assert frame.header(subscribe, "id") == Ok("late")
  assert next(events) == glomp.Subscribed("late")
  assert stop(client, peer) == glomp.Graceful
}

pub fn duplicate_subscription_ids_do_not_start_test() {
  let subscription =
    glomp.subscription(id: "a", destination: "/topic/a", handler: fn(message) {
      assert message.id == ""
    })

  let assert Error(error) =
    glomp.new(host: "127.0.0.1", port: 1)
    |> glomp.with_subscription(subscription)
    |> glomp.with_subscription(subscription)
    |> glomp.start

  assert error == actor.InitFailed("glomp: two subscriptions have the id a")
}

/// The client sends heart-beats.
pub fn sends_heart_beats_test() {
  let server = broker.start()
  let events = process.new_subject()

  let client =
    test_client(server.port, events)
    |> glomp.heart_beat(send_ms: 40, receive_ms: 0)
    |> start

  let #(connect, peer) =
    broker.accept_with(server.listener, [
      #("version", "1.2"),
      #("heart-beat", "0,40"),
    ])

  assert frame.header(connect, "heart-beat") == Ok("40,0")
  assert connected(events).heart_beat_send_ms == 40

  list.each([1, 2, 3], fn(beat) {
    assert broker.receive_bytes(peer.socket, 200) == Ok(<<"\n":utf8>>)
      as { "heart-beat " <> int.to_string(beat) }
  })

  assert stop(client, peer) == glomp.Graceful
}

/// A broker that stops sending is disconnected.
pub fn a_silent_broker_is_dropped_test() {
  let server = broker.start()
  let events = process.new_subject()

  let client =
    test_client(server.port, events)
    |> glomp.heart_beat(send_ms: 0, receive_ms: 40)
    |> start

  let #(connect, peer) =
    broker.accept_with(server.listener, [
      #("version", "1.2"),
      #("heart-beat", "40,0"),
    ])

  assert frame.header(connect, "heart-beat") == Ok("0,40")
  assert connected(events).heart_beat_receive_ms == 40

  // No disconnect while the broker sends heart-beats.
  list.each([1, 2, 3, 4, 5, 6], fn(beat) {
    broker.send_bytes(peer.socket, frame.heart_beat)

    assert receives_nothing(events, within: 30)
      as { "beat " <> int.to_string(beat) }
  })

  let assert glomp.Disconnected(glomp.HeartBeatMissed(idle_ms), 20) =
    next(events)

  assert idle_ms >= 80
  assert broker.closes(peer, within: 1000)

  let #(connect, peer) = broker.accept_stomp(server.listener)
  assert connect.command == frame.Connect
  assert connected(events).heart_beat_receive_ms == 0
  assert stop(client, peer) == glomp.Graceful
}

pub fn stop_test() {
  let server = broker.start()
  let events = process.new_subject()
  let #(client, peer) =
    test_client(server.port, events) |> connect(server, events)
  let stopped = async(fn() { glomp.stop(client) })
  let #(disconnect, peer) = broker.receive(peer)
  let assert Frame(frame.Disconnect, [#("receipt", receipt)], <<>>) = disconnect
  assert receipt == "glomp-disconnect"

  // The broker closes without sending the receipt.
  broker.close(peer.socket)

  assert next(stopped) == glomp.Abrupt(glomp.Closed)

  let #(client, peer) =
    test_client(server.port, events) |> connect(server, events)
  let stopped = async(fn() { glomp.stop(client) })
  let #(disconnect, peer) = broker.receive(peer)
  assert disconnect.command == frame.Disconnect
  // The broker sends no receipt. The client waits two seconds.
  assert receives_nothing(stopped, within: 1500)
  assert next(stopped) == glomp.Unconfirmed
  assert broker.closes(peer, within: 1000)

  // A message before the receipt does not prevent `Graceful`.
  let #(client, peer) =
    test_client(server.port, events) |> connect(server, events)
  let stopped = async(fn() { glomp.stop(client) })
  let #(disconnect, peer) = broker.receive(peer)

  broker.send_message(
    peer,
    [#("subscription", "a"), #("message-id", "1"), #("destination", "/a")],
    <<"late":utf8>>,
  )

  broker.send_receipt(peer, disconnect)

  assert next(stopped) == glomp.Graceful
  assert broker.closes(peer, within: 1000)
  // After `stop` the client does not reconnect.
  assert broker.nobody_connects(server.listener, within: 150)
}

/// A handler can call `send`.
pub fn a_handler_may_send_test() {
  let server = broker.start()
  let events = process.new_subject()
  let base = test_client(server.port, events)

  let #(client, peer) =
    base
    |> glomp.with_subscription(
      glomp.subscription(
        id: "in",
        destination: "/queue/in",
        handler: fn(message) {
          assert glomp.send(
              base,
              glomp.message(to: "/queue/out", body: message.body),
            )
            == Ok(Nil)
        },
      ),
    )
    |> connect(server, events)

  let #(subscribe, peer) = broker.receive(peer)
  assert subscribe.command == frame.Subscribe

  broker.send_message(
    peer,
    [
      #("subscription", "in"),
      #("message-id", "1"),
      #("destination", "/queue/in"),
    ],
    <<"ping":utf8>>,
  )

  let #(sent, peer) = broker.receive(peer)
  assert sent.command == frame.Send
  assert sent.body == <<"ping":utf8>>
  assert stop(client, peer) == glomp.Graceful
}

/// Slow handlers get messages in order. The client still replies.
pub fn slow_handlers_test() {
  let server = broker.start()
  let events = process.new_subject()
  let bodies = process.new_subject()

  let #(client, peer) =
    test_client(server.port, events)
    |> glomp.with_subscription(
      glomp.subscription(id: "a", destination: "/topic/a", handler: fn(message) {
        process.sleep(2)
        process.send(bodies, message.body)
      }),
    )
    |> connect(server, events)

  let #(subscribe, peer) = broker.receive(peer)
  assert subscribe.command == frame.Subscribe

  let count = 400
  let numbers = count_up(1, count)

  list.each(numbers, fn(number) {
    broker.send_message(
      peer,
      [
        #("subscription", "a"),
        #("message-id", int.to_string(number)),
        #("destination", "/topic/a"),
      ],
      <<int.to_string(number):utf8>>,
    )
  })

  // The client replies while handlers are busy.
  assert glomp.await_connected(client, within: 50) == Ok(Nil)

  assert glomp.send(client, glomp.message(to: "/queue/a", body: <<>>))
    == Ok(Nil)

  assert next(bodies) == <<"1":utf8>>

  assert list.map(list.drop(numbers, 1), fn(number) {
      assert next(bodies) == <<int.to_string(number):utf8>>
      number
    })
    == list.drop(numbers, 1)

  let #(sent, peer) = broker.receive(peer)
  assert sent.command == frame.Send
  assert stop(client, peer) == glomp.Graceful
}

/// A supervised client restarts, and sends DISCONNECT on shutdown.
pub fn supervised_test() {
  process.trap_exits(True)

  let server = broker.start()
  let events = process.new_subject()
  let client = test_client(server.port, events)

  let assert Ok(supervisor) =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(glomp.supervised(client))
    |> static_supervisor.start

  let #(connect, peer) = broker.accept_stomp(server.listener)
  assert connect.command == frame.Connect
  assert connected(events).version == frame.Stomp12

  let first = broker.only_child(supervisor.pid)
  let helpers = broker.linked_processes(first, except: supervisor.pid)
  assert list.length(helpers) == 1

  process.kill(first)

  assert broker.closes(peer, within: 1000)
  assert broker.all_exit(helpers, within: 1000)

  let #(connect, peer) = broker.accept_stomp(server.listener)
  assert connect.command == frame.Connect
  assert connected(events).version == frame.Stomp12

  let second = broker.only_child(supervisor.pid)
  assert second != first

  assert glomp.send(client, glomp.message(to: "/queue/a", body: <<>>))
    == Ok(Nil)

  let #(sent, peer) = broker.receive(peer)
  assert sent.command == frame.Send

  process.send_abnormal_exit(supervisor.pid, atom.create("shutdown"))

  let #(disconnect, peer) = broker.receive(peer)
  assert disconnect.command == frame.Disconnect

  broker.send_receipt(peer, disconnect)

  assert broker.closes(peer, within: 1000)
  assert broker.nobody_connects(server.listener, within: 150)
  assert process.is_alive(second) == False
}

/// A killed supervisor stops the client and its helpers.
pub fn a_killed_supervisor_stops_the_client_test() {
  process.trap_exits(True)

  let server = broker.start()
  let events = process.new_subject()
  let client = test_client(server.port, events)

  let assert Ok(supervisor) =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(glomp.supervised(client))
    |> static_supervisor.start

  let #(connect, peer) = broker.accept_stomp(server.listener)
  assert connect.command == frame.Connect
  assert connected(events).version == frame.Stomp12

  let first = broker.only_child(supervisor.pid)
  let helpers = broker.linked_processes(first, except: supervisor.pid)
  let monitor = process.monitor(first)

  process.kill(supervisor.pid)

  let down =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(down) { down })
    |> process.selector_receive(1000)

  assert down
    == Ok(process.ProcessDown(monitor:, pid: first, reason: process.Killed))

  assert broker.closes(peer, within: 1000)
  assert broker.all_exit(helpers, within: 1000)
  assert broker.nobody_connects(server.listener, within: 150)
}

/// A supervisor restarts a client stopped with `stop`.
pub fn a_supervisor_restarts_a_stopped_client_test() {
  process.trap_exits(True)

  let server = broker.start()
  let events = process.new_subject()
  let client = test_client(server.port, events)

  let assert Ok(supervisor) =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(glomp.supervised(client))
    |> static_supervisor.start

  let #(connect, peer) = broker.accept_stomp(server.listener)
  assert connect.command == frame.Connect
  assert connected(events).version == frame.Stomp12
  assert stop(client, peer) == glomp.Graceful

  let #(connect, peer) = broker.accept_stomp(server.listener)
  assert connect.command == frame.Connect
  assert connected(events).version == frame.Stomp12

  process.send_abnormal_exit(supervisor.pid, atom.create("shutdown"))

  let #(disconnect, peer) = broker.receive(peer)
  assert disconnect.command == frame.Disconnect

  broker.send_receipt(peer, disconnect)

  assert broker.closes(peer, within: 1000)
}

/// A transient supervised client stays stopped after `stop`.
pub fn a_transient_client_stays_stopped_test() {
  process.trap_exits(True)

  let server = broker.start()
  let events = process.new_subject()
  let client = test_client(server.port, events)

  let assert Ok(supervisor) =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(
      glomp.supervised(client) |> supervision.restart(supervision.Transient),
    )
    |> static_supervisor.start

  let #(connect, peer) = broker.accept_stomp(server.listener)
  assert connect.command == frame.Connect
  assert connected(events).version == frame.Stomp12
  assert stop(client, peer) == glomp.Graceful
  assert broker.nobody_connects(server.listener, within: 150)

  process.send_abnormal_exit(supervisor.pid, atom.create("shutdown"))
}

/// `stop` also stops the helper processes.
pub fn stop_stops_the_helpers_test() {
  let server = broker.start()
  let events = process.new_subject()
  let assert Ok(started) = glomp.start(test_client(server.port, events))
  let #(connect, peer) = broker.accept_stomp(server.listener)
  assert connect.command == frame.Connect
  assert connected(events).version == frame.Stomp12

  let helpers = broker.linked_processes(started.pid, except: process.self())
  assert list.length(helpers) == 1

  assert stop(started.data, peer) == glomp.Graceful
  assert process.is_alive(started.pid) == False
  assert broker.all_exit(helpers, within: 1000)
}

/// A handler crash restarts the client.
pub fn a_crashing_handler_restarts_the_client_test() {
  process.trap_exits(True)

  let server = broker.start()
  let events = process.new_subject()

  let client =
    test_client(server.port, events)
    |> glomp.with_subscription(
      glomp.subscription(id: "a", destination: "/topic/a", handler: fn(message) {
        assert message.body != <<"crash":utf8>>
      }),
    )

  let assert Ok(supervisor) =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(glomp.supervised(client))
    |> static_supervisor.start

  let #(connect, peer) = broker.accept_stomp(server.listener)
  assert connect.command == frame.Connect

  let #(subscribe, peer) = broker.receive(peer)
  assert subscribe.command == frame.Subscribe

  broker.send_message(
    peer,
    [#("subscription", "a"), #("message-id", "1"), #("destination", "/topic/a")],
    <<"crash":utf8>>,
  )

  // It sends DISCONNECT before exiting.
  let #(disconnect, peer) = broker.receive(peer)
  assert disconnect.command == frame.Disconnect

  broker.send_receipt(peer, disconnect)

  assert broker.closes(peer, within: 1000)

  let #(connect, peer) = broker.accept_stomp(server.listener)
  assert connect.command == frame.Connect

  let #(subscribe, peer) = broker.receive(peer)
  assert subscribe.command == frame.Subscribe

  process.send_abnormal_exit(supervisor.pid, atom.create("shutdown"))

  let #(disconnect, peer) = broker.receive(peer)
  assert disconnect.command == frame.Disconnect

  broker.send_receipt(peer, disconnect)

  assert broker.closes(peer, within: 1000)
}

pub fn tls_test() {
  let server = broker.start_tls()
  let events = process.new_subject()
  let messages = process.new_subject()

  let client =
    glomp.new(host: "localhost", port: server.port)
    |> glomp.heart_beat(send_ms: 0, receive_ms: 0)
    |> glomp.reconnect(first_ms: 20, longest_ms: 80)
    |> glomp.on_event(process.send(events, _))
    |> glomp.with_subscription(
      glomp.subscription(
        id: "a",
        destination: "/topic/a",
        handler: process.send(messages, _),
      ),
    )

  let trusted =
    client
    |> glomp.tls(trust: network.CertificateFile(server.authority))
    |> start

  let #(connect, peer) = broker.accept_stomp(server.listener)
  assert connect.command == frame.Connect
  assert connected(events).version == frame.Stomp12

  let #(subscribe, peer) = broker.receive_subscribe(peer)
  assert subscribe.command == frame.Subscribe
  assert next(events) == glomp.Subscribed("a")

  broker.send_message(
    peer,
    [#("subscription", "a"), #("message-id", "1"), #("destination", "/topic/a")],
    <<"secret":utf8>>,
  )

  assert next(messages).body == <<"secret":utf8>>

  assert glomp.send(trusted, glomp.message(to: "/queue/a", body: <<"x":utf8>>))
    == Ok(Nil)

  let #(sent, peer) = broker.receive(peer)
  assert sent.body == <<"x":utf8>>
  assert stop(trusted, peer) == glomp.Graceful

  let unverified = client |> glomp.tls(trust: network.Unverified) |> start
  let #(connect, peer) = broker.accept_stomp(server.listener)
  assert connect.command == frame.Connect
  assert connected(events).version == frame.Stomp12

  let #(subscribe, peer) = broker.receive(peer)
  assert subscribe.command == frame.Subscribe
  assert stop(unverified, peer) == glomp.Graceful
}

fn count_up(from: Int, to: Int) -> List(Int) {
  case from > to {
    True -> []
    False -> [from, ..count_up(from + 1, to)]
  }
}

/// `stop` replies to every caller waiting on a receipt.
pub fn stop_answers_pending_receipts_test() {
  let server = broker.start()
  let events = process.new_subject()
  let #(client, peer) =
    test_client(server.port, events) |> connect(server, events)
  let outbound = glomp.message(to: "/queue/a", body: <<>>)

  let first =
    async(fn() { glomp.send_confirmed(client, outbound, within: 5000) })

  let #(first_send, peer) = broker.receive(peer)

  let second =
    async(fn() { glomp.send_confirmed(client, outbound, within: 5000) })

  let #(second_send, peer) = broker.receive(peer)
  assert second_send.command == frame.Send

  let stopped = async(fn() { glomp.stop(client) })
  let #(disconnect, peer) = broker.receive(peer)

  broker.send_receipt(peer, first_send)
  broker.send_receipt(peer, disconnect)

  assert next(stopped) == glomp.Graceful
  assert next(first) == Ok(Nil)
  assert next(second) == Error(glomp.ConnectionLost(glomp.Closed))
  assert broker.closes(peer, within: 1000)
}

/// Stopping while connecting closes the socket.
pub fn stop_while_connecting_test() {
  let server = broker.start()
  let events = process.new_subject()
  let client = test_client(server.port, events) |> start
  let #(connect, peer) = broker.receive(broker.accept(server.listener))
  assert connect.command == frame.Connect
  assert glomp.stop(client) == glomp.Idle
  assert broker.closes(peer, within: 1000)
  assert receives_nothing(events, within: 50)
}

/// A client stops when its parent exits, sending DISCONNECT.
pub fn stops_with_its_parent_test() {
  let server = broker.start()
  let events = process.new_subject()
  let release = process.new_subject()

  process.spawn_unlinked(fn() {
    let gate = process.new_subject()
    let client = test_client(server.port, events) |> start

    process.send(release, #(gate, client))
    process.receive_forever(gate)
  })

  let #(gate, client) = next(release)
  let #(connect, peer) = broker.accept_stomp(server.listener)
  assert connect.command == frame.Connect
  assert connected(events).version == frame.Stomp12
  assert glomp.await_connected(client, within: 100) == Ok(Nil)

  process.send(gate, Nil)

  let #(disconnect, peer) = broker.receive(peer)
  assert disconnect.command == frame.Disconnect

  broker.send_receipt(peer, disconnect)

  assert broker.closes(peer, within: 1000)
  assert broker.nobody_connects(server.listener, within: 150)
}

/// A stopped client can be started again immediately.
pub fn a_stopped_client_can_be_started_again_test() {
  let client = glomp.new(host: "127.0.0.1", port: broker.free_port())

  list.each([1, 2, 3], fn(round) {
    let client = start(client)
    assert glomp.stop(client) == glomp.Idle as { int.to_string(round) }
  })
}
