//// The client against the STOMP 1.2 specification.

import broker
import gleam/bit_array
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option.{None}
import gleam/string
import glomp
import glomp/frame.{Frame}
import glomp/network
import harness.{
  async, connect, connected, next, receives_nothing, start, stop, test_client,
  to,
}

// --------------------------------------------------------------- connecting

/// CONNECT is never escaped.
pub fn connect_is_not_escaped_test() {
  let server = broker.start()
  let events = process.new_subject()

  let client =
    test_client(server.port, events)
    |> glomp.virtual_host("a:b\\c")
    |> glomp.credentials(login: "us:er", passcode: "p\\ss")
    |> start

  let peer = broker.accept(server.listener)
  let assert Ok(raw) = broker.receive_bytes(peer.socket, 5000)

  assert raw
    == <<
      "CONNECT\naccept-version:1.1,1.2\nhost:a:b\\c\nlogin:us:er\npasscode:p\\ss\nheart-beat:0,0\n\n":utf8,
      0,
    >>

  broker.send(peer, Frame(frame.Connected, [#("version", "1.2")], <<>>))

  assert connected(events).version == frame.Stomp12
  assert stop(client, peer) == glomp.Graceful
}

/// Each direction: 0 if either is 0, else the larger.
pub fn heart_beat_negotiation_test() {
  let server = broker.start()

  let cases = [
    #(#(0, 0), [#("heart-beat", "0,0")], #(0, 0)),
    #(#(1000, 2000), [#("heart-beat", "3000,4000")], #(4000, 3000)),
    #(#(5000, 6000), [#("heart-beat", "3000,4000")], #(5000, 6000)),
    #(#(1000, 2000), [#("heart-beat", "0,0")], #(0, 0)),
    #(#(0, 0), [#("heart-beat", "100,100")], #(0, 0)),
    #(#(5000, 0), [#("heart-beat", "100,100")], #(5000, 0)),
    #(#(0, 5000), [#("heart-beat", "100,100")], #(0, 5000)),
    #(#(5000, 5000), [#("heart-beat", "0,100")], #(5000, 0)),
    #(#(5000, 5000), [#("heart-beat", "100,0")], #(0, 5000)),
    // A missing header means 0,0.
    #(#(5000, 5000), [], #(0, 0)),
    // An invalid header means 0,0.
    #(#(5000, 5000), [#("heart-beat", "soon")], #(0, 0)),
    #(#(5000, 5000), [#("heart-beat", "100")], #(0, 0)),
    #(#(5000, 5000), [#("heart-beat", "100,")], #(0, 0)),
    #(#(5000, 5000), [#("heart-beat", ",100")], #(0, 0)),
  ]

  list.each(cases, fn(row) {
    let #(#(send_ms, receive_ms), reply, #(sends, expects)) = row
    let events = process.new_subject()

    let client =
      test_client(server.port, events)
      |> glomp.heart_beat(send_ms:, receive_ms:)
      |> start

    let #(connect, peer) =
      broker.accept_with(server.listener, [#("version", "1.2"), ..reply])

    assert frame.header(connect, "heart-beat")
      == Ok(int.to_string(send_ms) <> "," <> int.to_string(receive_ms))

    let session = connected(events)

    assert #(session.heart_beat_send_ms, session.heart_beat_receive_ms)
      == #(sends, expects)
      as { string.inspect(row) }

    assert stop(client, peer) == glomp.Graceful
  })
}

/// Negative heart-beats are sent as 0.
pub fn negative_heart_beats_are_none_test() {
  let server = broker.start()
  let events = process.new_subject()

  let client =
    test_client(server.port, events)
    |> glomp.heart_beat(send_ms: -5, receive_ms: -1)
    |> start

  let #(connect, peer) = broker.accept_stomp(server.listener)
  assert frame.header(connect, "heart-beat") == Ok("0,0")
  assert connected(events).heart_beat_send_ms == 0
  assert stop(client, peer) == glomp.Graceful
}

/// A broker may end lines with CRLF, handshake included.
pub fn a_crlf_broker_test() {
  let server = broker.start()
  let events = process.new_subject()
  let messages = process.new_subject()

  let client =
    test_client(server.port, events)
    |> glomp.with_subscription(
      glomp.subscription(
        id: "a",
        destination: "/topic/a",
        handler: process.send(messages, _),
      ),
    )
    |> start

  let #(connect, peer) = broker.receive(broker.accept(server.listener))
  assert connect.command == frame.Connect

  broker.send_bytes(peer.socket, <<
    "CONNECTED\r\nversion:1.2\r\nserver:crlf/1\r\n\r\n":utf8, 0, "\r\n":utf8,
  >>)

  assert connected(events).server == option_some("crlf/1")

  let #(subscribe, peer) = broker.receive(peer)
  assert subscribe.command == frame.Subscribe

  broker.send_bytes(peer.socket, <<
    "MESSAGE\r\nsubscription:a\r\nmessage-id:1\r\ndestination:/topic/a\r\n\r\nline\r\n":utf8,
    0,
    "\r\n":utf8,
  >>)

  let message = next(messages)
  assert message.body == <<"line\r\n":utf8>>
  assert message.destination == "/topic/a"
  assert stop(client, peer) == glomp.Graceful
}

fn option_some(value: a) -> option.Option(a) {
  option.Some(value)
}

/// Frames in the same packet as CONNECTED are handled.
pub fn frames_behind_connected_test() {
  let server = broker.start()
  let seen = process.new_subject()

  let client =
    glomp.new(host: "127.0.0.1", port: server.port)
    |> glomp.heart_beat(send_ms: 0, receive_ms: 0)
    |> glomp.on_event(fn(event) {
      case event {
        glomp.Connected(session:) ->
          process.send(seen, "connected " <> string.inspect(session.version))

        glomp.Subscribed(id:) -> process.send(seen, "subscribed " <> id)

        glomp.Disconnected(reason:, retry_in_ms:) ->
          process.send(
            seen,
            string.inspect(reason) <> int.to_string(retry_in_ms),
          )

        glomp.Dropped(frame: dropped, reason:) ->
          process.send(seen, string.inspect(#(dropped, reason)))
      }
    })
    |> glomp.with_subscription(
      glomp.subscription(id: "a", destination: "/topic/a", handler: fn(message) {
        process.send(seen, "message " <> message.id)
      }),
    )
    |> start

  let #(connect, peer) = broker.receive(broker.accept(server.listener))
  assert connect.command == frame.Connect

  let encode = frame.encode(_, frame.Stomp12)

  broker.send_bytes(
    peer.socket,
    bit_array.concat([
      encode(Frame(frame.Connected, [#("version", "1.2")], <<>>)),
      encode(Frame(frame.Message, to("a", "1"), <<>>)),
      encode(Frame(frame.Message, to("a", "2"), <<>>)),
    ]),
  )

  assert [next(seen), next(seen), next(seen)]
    == ["connected Stomp12", "message 1", "message 2"]

  let #(subscribe, peer) = broker.receive(peer)
  assert subscribe.command == frame.Subscribe
  assert stop(client, peer) == glomp.Graceful
}

// ---------------------------------------------------------------- receiving

/// A frame split across packets is handled once.
pub fn a_frame_in_pieces_test() {
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

  let encoded =
    frame.encode(
      Frame(frame.Message, [#("content-length", "11"), ..to("a", "1")], <<
        "hello",
        0,
        "world",
      >>),
      frame.Stomp12,
    )

  list.each(pieces(encoded, 7), fn(piece) {
    assert receives_nothing(messages, within: 3)

    broker.send_bytes(peer.socket, piece)
  })

  assert next(messages).body == <<"hello", 0, "world">>
  assert receives_nothing(messages, within: 30)
  assert stop(client, peer) == glomp.Graceful
}

fn pieces(bytes: BitArray, size: Int) -> List(BitArray) {
  case bytes {
    <<piece:bytes-size(size), rest:bytes>> -> [piece, ..pieces(rest, size)]
    <<>> -> []
    last -> [last]
  }
}

/// Many frames in one packet are handled in order.
pub fn many_frames_in_one_packet_test() {
  let server = broker.start()
  let events = process.new_subject()
  let ids = process.new_subject()

  let #(client, peer) =
    test_client(server.port, events)
    |> glomp.with_subscription(
      glomp.subscription(id: "a", destination: "/topic/a", handler: fn(message) {
        process.send(ids, message.id)
      }),
    )
    |> connect(server, events)

  let #(subscribe, peer) = broker.receive(peer)
  assert subscribe.command == frame.Subscribe

  let sent = list.map(count_up(1, 300), int.to_string)

  broker.send_bytes(
    peer.socket,
    list.map(sent, fn(id) {
      frame.encode(
        Frame(frame.Message, to("a", id), <<id:utf8>>),
        frame.Stomp12,
      )
    })
      |> list.intersperse(frame.heart_beat)
      |> bit_array.concat,
  )

  assert list.map(sent, fn(id) {
      assert next(ids) == id
      id
    })
    == sent

  assert stop(client, peer) == glomp.Graceful
}

fn count_up(from: Int, to: Int) -> List(Int) {
  case from > to {
    True -> []
    False -> [from, ..count_up(from + 1, to)]
  }
}

/// Headers are unescaped. Bodies are unchanged.
pub fn messages_arrive_decoded_test() {
  let server = broker.start()
  let events = process.new_subject()
  let messages = process.new_subject()

  let #(client, peer) =
    test_client(server.port, events)
    |> glomp.with_subscription(
      glomp.subscription(
        id: "a:b",
        destination: "/topic/a",
        handler: process.send(messages, _),
      ),
    )
    |> connect(server, events)

  let #(subscribe, peer) = broker.receive(peer)
  assert frame.header(subscribe, "id") == Ok("a:b")

  let body = <<0, 255, "\n\n", 0, "MESSAGE">>

  broker.send_bytes(peer.socket, <<
    "MESSAGE\nsubscription:a\\cb\nmessage-id:m\\n1\ndestination:/topic/a\\\\b\nx\\ck:line\\r\\n\ncontent-length:12\n\n":utf8,
    body:bits,
    0,
  >>)

  let message = next(messages)
  assert message.subscription == "a:b"
  assert message.id == "m\n1"
  assert message.destination == "/topic/a\\b"
  assert glomp.header(message, "x:k") == Ok("line\r\n")
  assert message.body == body
  assert stop(client, peer) == glomp.Graceful
}

/// Each handler receives only its own subscription's messages.
pub fn messages_go_to_their_subscription_test() {
  let server = broker.start()
  let events = process.new_subject()
  let seen = process.new_subject()

  let subscription = fn(id) {
    glomp.subscription(id:, destination: "/topic/" <> id, handler: fn(message) {
      process.send(seen, #(id, message.id))
    })
  }

  let #(client, peer) =
    test_client(server.port, events)
    |> glomp.with_subscription(subscription("one"))
    |> glomp.with_subscription(subscription("two"))
    |> glomp.with_subscription(subscription("three"))
    |> connect(server, events)

  let #(first, peer) = broker.receive(peer)
  let #(second, peer) = broker.receive(peer)
  let #(third, peer) = broker.receive(peer)

  // SUBSCRIBE frames are sent in the order added.
  assert list.map([first, second, third], frame.header(_, "id"))
    == [Ok("one"), Ok("two"), Ok("three")]

  let order = ["two", "one", "three", "three", "one", "two", "two"]

  list.index_map(order, fn(id, index) {
    broker.send_message(peer, to(id, int.to_string(index)), <<>>)
  })

  assert list.index_map(order, fn(id, index) {
      assert next(seen) == #(id, int.to_string(index))
      id
    })
    == order

  assert stop(client, peer) == glomp.Graceful
}

/// Any frame resets the heart-beat timeout.
pub fn frames_count_as_heart_beats_test() {
  let server = broker.start()
  let events = process.new_subject()
  let messages = process.new_subject()

  let client =
    test_client(server.port, events)
    |> glomp.heart_beat(send_ms: 0, receive_ms: 40)
    |> glomp.with_subscription(
      glomp.subscription(
        id: "a",
        destination: "/topic/a",
        handler: process.send(messages, _),
      ),
    )
    |> start

  let #(connect, peer) =
    broker.accept_with(server.listener, [
      #("version", "1.2"),
      #("heart-beat", "40,0"),
    ])

  assert connect.command == frame.Connect
  assert connected(events).heart_beat_receive_ms == 40

  let #(subscribe, peer) = broker.receive(peer)
  assert subscribe.command == frame.Subscribe

  list.each(count_up(1, 8), fn(number) {
    broker.send_message(peer, to("a", int.to_string(number)), <<>>)

    assert next(messages).id == int.to_string(number)
    assert receives_nothing(events, within: 30)
  })

  assert stop(client, peer) == glomp.Graceful
}

// ------------------------------------------------------------------ sending

/// Caller-added headers are escaped.
pub fn sent_headers_are_escaped_test() {
  let server = broker.start()
  let events = process.new_subject()
  let #(client, peer) =
    test_client(server.port, events) |> connect(server, events)

  assert glomp.send(
      client,
      glomp.message(to: "/queue/a:b", body: <<"x":utf8>>)
        |> glomp.content_type("text/plain;charset=utf-8")
        |> glomp.message_header(name: "k:1", value: "a\nb\\c\rd"),
    )
    == Ok(Nil)

  let assert Ok(raw) = broker.receive_bytes(peer.socket, 5000)

  assert raw
    == <<
      "SEND\ndestination:/queue/a\\cb\ncontent-type:text/plain;charset=utf-8\ncontent-length:1\nk\\c1:a\\nb\\\\c\\rd\n\nx":utf8,
      0,
    >>

  assert stop(client, peer) == glomp.Graceful
}

/// On 1.1 a carriage return is sent unescaped.
pub fn sent_headers_on_1_1_test() {
  let server = broker.start()
  let events = process.new_subject()
  let client = test_client(server.port, events) |> start

  let #(connect, peer) =
    broker.accept_with(server.listener, [#("version", "1.1")])

  assert connect.command == frame.Connect
  assert connected(events).version == frame.Stomp11

  assert glomp.send(
      client,
      glomp.message(to: "/queue/a", body: <<>>)
        |> glomp.without_content_length
        |> glomp.message_header(name: "k", value: "a:b\rc"),
    )
    == Ok(Nil)

  let assert Ok(raw) = broker.receive_bytes(peer.socket, 5000)
  assert raw == <<"SEND\ndestination:/queue/a\nk:a\\cb\rc\n\n":utf8, 0>>
  assert stop(client, peer) == glomp.Graceful
}

/// A 1 MiB body is sent with its content-length.
pub fn a_large_message_test() {
  let server = broker.start()
  let events = process.new_subject()
  let #(client, peer) =
    test_client(server.port, events) |> connect(server, events)
  let body = <<string.repeat("0123456789abcde\u{0}", 65_536):utf8>>
  assert glomp.send(client, glomp.message(to: "/queue/big", body:)) == Ok(Nil)

  let #(sent, peer) = broker.receive(peer)
  assert frame.header(sent, "content-length") == Ok("1048576")
  assert sent.body == body
  assert stop(client, peer) == glomp.Graceful
}

/// Receipt ids are unique across frames and connections.
pub fn receipt_ids_are_unique_test() {
  let server = broker.start()
  let events = process.new_subject()
  let outbound = glomp.message(to: "/queue/a", body: <<>>)

  let wanted =
    glomp.subscription(id: "a", destination: "/topic/a", handler: fn(message) {
      assert message.id != ""
    })

  let #(client, peer) =
    test_client(server.port, events)
    |> glomp.with_subscription(wanted)
    |> connect(server, events)

  let round = fn(peer) {
    let #(subscribe, peer) = broker.receive(peer)
    let confirmed = async(fn() { glomp.send_confirmed(client, outbound, 5000) })
    let #(send, peer) = broker.receive(peer)

    broker.send_receipt(peer, send)

    assert next(confirmed) == Ok(Nil)

    let assert Ok(transaction) = glomp.begin(client)
    let #(begin, peer) = broker.receive(peer)
    let committed = async(fn() { glomp.commit(client, transaction, 5000) })
    let #(commit, peer) = broker.receive(peer)

    broker.send_receipt(peer, commit)

    assert next(committed) == Ok(Nil)

    let assert Ok(transaction_id) = frame.header(begin, "transaction")

    #(
      list.filter_map([subscribe, send, commit], frame.header(_, "receipt")),
      transaction_id,
      peer,
    )
  }

  let #(first, first_transaction, peer) = round(peer)

  broker.close(peer.socket)

  assert next(events) == glomp.Disconnected(glomp.Closed, 20)

  let #(connect, peer) = broker.accept_stomp(server.listener)
  assert connect.command == frame.Connect
  assert connected(events).version == frame.Stomp12

  let #(second, second_transaction, peer) = round(peer)
  let ids = list.append(first, second)
  assert list.length(ids) == 6
  assert list.unique(ids) == ids
  assert first_transaction != second_transaction
  assert stop(client, peer) == glomp.Graceful
}

/// Each waiting caller gets its own receipt.
pub fn receipts_reach_their_callers_test() {
  let server = broker.start()
  let events = process.new_subject()
  let #(client, peer) =
    test_client(server.port, events) |> connect(server, events)

  let #(waiting, peer) =
    list.fold(count_up(1, 20), #([], peer), fn(so_far, number) {
      let body = <<int.to_string(number):utf8>>

      let outcome =
        async(fn() {
          glomp.send_confirmed(
            client,
            glomp.message(to: "/queue/a", body:),
            within: 5000,
          )
          |> pair(number)
        })

      let #(send, peer) = broker.receive(so_far.1)
      assert send.body == body

      #([#(send, outcome), ..so_far.0], peer)
    })

  // Confirm the even-numbered sends, newest first.
  list.each(waiting, fn(sent) {
    case sent.0.body, int.parse(text(sent.0.body)) {
      body, Ok(number) if number % 2 == 0 -> {
        assert body != <<>>

        broker.send_receipt(peer, sent.0)

        assert next(sent.1) == #(number, Ok(Nil))
      }

      body, parsed -> {
        assert body != <<>> && parsed != Error(Nil)
        assert receives_nothing(sent.1, within: 0)
      }
    }
  })

  // The others get `ConnectionLost` when the connection closes.
  broker.close(peer.socket)

  list.each(waiting, fn(sent) {
    case int.parse(text(sent.0.body)) {
      Ok(number) if number % 2 == 1 -> {
        assert next(sent.1)
          == #(number, Error(glomp.ConnectionLost(glomp.Closed)))
      }

      parsed -> {
        assert parsed != Error(Nil)
      }
    }
  })

  assert glomp.stop(client) == glomp.Idle
}

fn pair(second: b, first: a) -> #(a, b) {
  #(first, second)
}

fn text(bytes: BitArray) -> String {
  let assert Ok(text) = bit_array.to_string(bytes)

  text
}

// ----------------------------------------------------------- acknowledging

/// In `client` mode one ACK covers everything before it.
pub fn a_cumulative_ack_is_one_frame_test() {
  let server = broker.start()
  let events = process.new_subject()

  let #(client, peer) =
    test_client(server.port, events)
    |> glomp.with_subscription(
      glomp.acknowledged_subscription(
        id: "q",
        destination: "/queue/q",
        mode: glomp.Cumulative,
        handler: fn(message, delivery) {
          case message.id == "3", delivery {
            True, covers_all -> {
              assert covers_all == delivery

              glomp.Ack
            }

            False, later -> {
              assert later == delivery

              glomp.Defer
            }
          }
        },
      ),
    )
    |> connect(server, events)

  let #(subscribe, peer) = broker.receive(peer)
  assert frame.header(subscribe, "ack") == Ok("client")

  list.each(["1", "2", "3"], fn(id) {
    broker.send_message(peer, [#("ack", "ack-" <> id), ..to("q", id)], <<>>)
  })

  let #(settled, peer) = broker.receive(peer)
  assert settled == Frame(frame.Ack, [#("id", "ack-3")], <<>>)

  assert glomp.send(client, glomp.message(to: "/queue/a", body: <<>>))
    == Ok(Nil)

  // No ACK was sent for the first two.
  let #(following, peer) = broker.receive(peer)
  assert following.command == frame.Send
  assert stop(client, peer) == glomp.Graceful
}

/// NACK on 1.1 names message and subscription.
pub fn nack_on_1_1_test() {
  let server = broker.start()
  let events = process.new_subject()
  let deferred = process.new_subject()

  let client =
    test_client(server.port, events)
    |> glomp.with_subscription(
      glomp.acknowledged_subscription(
        id: "q",
        destination: "/queue/q",
        mode: glomp.Cumulative,
        handler: fn(message, delivery) {
          case message.id {
            "now" -> {
              process.send(deferred, #("ignored", delivery))
              glomp.Nack
            }

            later -> {
              process.send(deferred, #(later, delivery))
              glomp.Defer
            }
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

  broker.send_message(peer, to("q", "now"), <<>>)

  let #(settled, peer) = broker.receive(peer)

  assert settled
    == Frame(frame.Nack, [#("message-id", "now"), #("subscription", "q")], <<>>)

  assert next(deferred).0 == "ignored"

  broker.send_message(peer, to("q", "later"), <<>>)

  let #(id, delivery) = next(deferred)
  assert id == "later"
  assert glomp.nack(client, delivery) == Ok(Nil)

  let #(settled, peer) = broker.receive(peer)

  assert settled
    == Frame(
      frame.Nack,
      [#("message-id", "later"), #("subscription", "q")],
      <<>>,
    )

  assert stop(client, peer) == glomp.Graceful
}

/// A verdict for a closed connection is not sent.
pub fn a_late_verdict_is_not_sent_test() {
  let server = broker.start()
  let events = process.new_subject()
  let gates = process.new_subject()

  let #(client, peer) =
    test_client(server.port, events)
    |> glomp.with_subscription(
      glomp.acknowledged_subscription(
        id: "q",
        destination: "/queue/q",
        mode: glomp.Individual,
        handler: fn(message, delivery) {
          let gate = process.new_subject()

          process.send(gates, #(gate, message.id, delivery))
          process.receive_forever(gate)
        },
      ),
    )
    |> connect(server, events)

  let #(subscribe, peer) = broker.receive(peer)
  assert subscribe.command == frame.Subscribe

  broker.send_message(peer, [#("ack", "old"), ..to("q", "1")], <<>>)

  let #(gate, id, expired) = next(gates)
  assert id == "1"

  broker.close(peer.socket)

  let #(connect, peer) = broker.accept_stomp(server.listener)
  assert connect.command == frame.Connect

  let #(subscribe, peer) = broker.receive(peer)
  assert subscribe.command == frame.Subscribe

  // The handler returns after the reconnect.
  process.send(gate, glomp.Ack)

  assert next(events) == glomp.Disconnected(glomp.Closed, 20)
  assert connected(events).version == frame.Stomp12
  assert glomp.ack(client, expired) == Error(glomp.Expired)

  broker.send_message(peer, [#("ack", "new"), ..to("q", "2")], <<>>)

  let #(gate, id, fresh) = next(gates)
  assert id == "2"
  assert fresh != expired

  process.send(gate, glomp.Ack)

  // The new connection's first frame is the new ACK.
  let #(settled, peer) = broker.receive(peer)
  assert settled == Frame(frame.Ack, [#("id", "new")], <<>>)
  assert stop(client, peer) == glomp.Graceful
}

// ------------------------------------------------------------ subscriptions

/// An id that was unsubscribed can be subscribed again.
pub fn resubscribing_an_id_test() {
  let server = broker.start()
  let events = process.new_subject()
  let messages = process.new_subject()

  let wanted =
    glomp.subscription(id: "a", destination: "/topic/a", handler: process.send(
      messages,
      _,
    ))

  let #(client, peer) =
    test_client(server.port, events)
    |> glomp.with_subscription(wanted)
    |> connect(server, events)

  let #(subscribe, peer) = broker.receive_subscribe(peer)
  assert frame.header(subscribe, "id") == Ok("a")
  assert next(events) == glomp.Subscribed("a")
  assert glomp.unsubscribe(client, "a") == Ok(Nil)

  let #(unsubscribe, peer) = broker.receive(peer)
  assert unsubscribe.command == frame.Unsubscribe
  assert glomp.subscribe(client, wanted) == Ok(Nil)

  let #(again, peer) = broker.receive_subscribe(peer)

  assert broker.headers_but_receipt(again)
    == broker.headers_but_receipt(subscribe)

  assert next(events) == glomp.Subscribed("a")

  broker.send_message(peer, to("a", "1"), <<>>)

  assert next(messages).id == "1"
  assert stop(client, peer) == glomp.Graceful
}

/// Unsubscribing before the receipt emits no `Subscribed` event.
pub fn a_dropped_subscription_is_not_confirmed_test() {
  let server = broker.start()
  let events = process.new_subject()

  let #(client, peer) =
    test_client(server.port, events)
    |> glomp.with_subscription(
      glomp.subscription(id: "a", destination: "/topic/a", handler: fn(message) {
        assert message.id != ""
      }),
    )
    |> connect(server, events)

  let #(subscribe, peer) = broker.receive(peer)
  assert glomp.unsubscribe(client, "a") == Ok(Nil)

  let #(unsubscribe, peer) = broker.receive(peer)
  assert unsubscribe.command == frame.Unsubscribe

  broker.send_receipt(peer, subscribe)

  let assert glomp.Dropped(Frame(frame.Receipt, headers, <<>>), reason) =
    next(events)

  assert Ok(headers) == result_map(frame.header(subscribe, "receipt"))

  assert reason
    == glomp.UnknownReceipt(
      text(<<
        list.fold(headers, "", fn(id, header) { id <> header.1 }):utf8,
      >>),
    )

  assert stop(client, peer) == glomp.Graceful
}

fn result_map(
  receipt: Result(String, Nil),
) -> Result(List(#(String, String)), Nil) {
  case receipt {
    Ok(id) -> Ok([#("receipt-id", id)])
    Error(Nil) -> Error(Nil)
  }
}

// ------------------------------------------------------------- disconnecting

/// Nothing is sent after DISCONNECT, not even a heart-beat.
pub fn nothing_follows_disconnect_test() {
  let server = broker.start()
  let events = process.new_subject()

  let client =
    test_client(server.port, events)
    |> glomp.heart_beat(send_ms: 20, receive_ms: 0)
    |> start

  let #(connect, peer) =
    broker.accept_with(server.listener, [
      #("version", "1.2"),
      #("heart-beat", "0,20"),
    ])

  assert connect.command == frame.Connect
  assert connected(events).heart_beat_send_ms == 20
  // Heart-beats are being sent.
  assert broker.receive_bytes(peer.socket, 500) == Ok(<<"\n":utf8>>)

  let stopped = async(fn() { glomp.stop(client) })
  // Wait 300ms, longer than several heart-beat intervals.
  let sent = broker.receive_all_bytes(peer.socket, 300, <<>>)

  let disconnect =
    frame.encode(
      Frame(frame.Disconnect, [#("receipt", "glomp-disconnect")], <<>>),
      frame.Stomp12,
    )

  let size = bit_array.byte_size(sent) - bit_array.byte_size(disconnect)
  let assert Ok(before) = bit_array.slice(sent, 0, size)

  let assert Ok(last) =
    bit_array.slice(sent, size, bit_array.byte_size(disconnect))

  // Only heart-beats came before DISCONNECT, and nothing after.
  assert last == disconnect
  assert before == <<string.repeat("\n", size):utf8>>

  broker.send(
    peer,
    Frame(frame.Receipt, [#("receipt-id", "glomp-disconnect")], <<>>),
  )

  assert next(stopped) == glomp.Graceful
  assert broker.closes(peer, within: 1000)
}

/// Stopping does not wait for a running handler.
pub fn stop_does_not_wait_for_handlers_test() {
  let server = broker.start()
  let events = process.new_subject()
  let started = process.new_subject()

  let #(client, peer) =
    test_client(server.port, events)
    |> glomp.with_subscription(
      glomp.subscription(id: "a", destination: "/topic/a", handler: fn(message) {
        process.send(started, #(process.self(), message.id))
        process.sleep_forever()
      }),
    )
    |> connect(server, events)

  let #(subscribe, peer) = broker.receive(peer)
  assert subscribe.command == frame.Subscribe

  broker.send_message(peer, to("a", "1"), <<>>)

  let #(handler, id) = next(started)
  assert id == "1"
  assert process.is_alive(handler)
  assert stop(client, peer) == glomp.Graceful

  process.sleep(20)

  assert process.is_alive(handler) == False
}

// ------------------------------------------------- heart-beats and reconnects

/// Heart-beats are sent while a handler is running.
pub fn heart_beats_while_a_handler_works_test() {
  let server = broker.start()
  let events = process.new_subject()
  let gates = process.new_subject()

  let client =
    test_client(server.port, events)
    |> glomp.heart_beat(send_ms: 40, receive_ms: 0)
    |> glomp.with_subscription(
      glomp.subscription(id: "a", destination: "/topic/a", handler: fn(message) {
        let gate = process.new_subject()

        process.send(gates, #(gate, message.id))
        process.receive_forever(gate)
      }),
    )
    |> start

  let #(connect, peer) =
    broker.accept_with(server.listener, [
      #("version", "1.2"),
      #("heart-beat", "0,40"),
    ])

  assert connect.command == frame.Connect
  assert connected(events).heart_beat_send_ms == 40

  let #(subscribe, peer) = broker.receive(peer)
  assert subscribe.command == frame.Subscribe

  broker.send_message(peer, to("a", "1"), <<>>)

  let #(gate, id) = next(gates)
  assert id == "1"

  list.each(count_up(1, 5), fn(beat) {
    assert broker.receive_bytes(peer.socket, 200) == Ok(<<"\n":utf8>>)
      as { "heart-beat " <> int.to_string(beat) }
  })

  process.send(gate, Nil)

  assert stop(client, peer) == glomp.Graceful
}

/// With handlers blocked, the broker's sends eventually time out.
pub fn back_pressure_reaches_the_broker_test() {
  let server = broker.start()
  let events = process.new_subject()
  let gates = process.new_subject()

  let #(client, peer) =
    test_client(server.port, events)
    |> glomp.with_subscription(
      glomp.subscription(id: "a", destination: "/topic/a", handler: fn(message) {
        let gate = process.new_subject()

        process.send(gates, #(gate, message.id))
        process.receive_forever(gate)
      }),
    )
    |> connect(server, events)

  let #(subscribe, peer) = broker.receive(peer)
  assert subscribe.command == frame.Subscribe

  let big =
    frame.encode(
      Frame(frame.Message, to("a", "big"), <<string.repeat("x", 65_536):utf8>>),
      frame.Stomp12,
    )

  // Up to 256 MiB.
  let written = write_until_blocked(peer.socket, big, 0, 4096)
  assert written < 4096
  // The client still replies.
  assert glomp.await_connected(client, within: 100) == Ok(Nil)

  let #(gate, id) = next(gates)
  assert id == "big"

  process.send(gate, Nil)
  broker.reset(peer.socket)

  assert list.contains(
    [glomp.Idle, glomp.Abrupt(glomp.Closed), glomp.Unconfirmed],
    glomp.stop(client),
  )
}

fn write_until_blocked(
  socket: broker.Socket,
  bytes: BitArray,
  written: Int,
  most: Int,
) -> Int {
  case written < most && broker.try_send_bytes(socket, bytes, 300) {
    True -> write_until_blocked(socket, bytes, written + 1, most)
    False -> written
  }
}

/// Waits double after short connections, reset after a long one.
pub fn back_off_starts_over_after_a_good_connection_test() {
  let server = broker.start()
  let events = process.new_subject()

  let client =
    test_client(server.port, events)
    |> glomp.reconnect(first_ms: 20, longest_ms: 60)
    |> start

  let connection_lasting = fn(milliseconds) {
    let #(connect, peer) = broker.accept_stomp(server.listener)
    assert connect.command == frame.Connect
    assert connected(events).version == frame.Stomp12

    process.sleep(milliseconds)
    broker.close(peer.socket)

    let assert glomp.Disconnected(glomp.Closed, wait) = next(events)

    wait
  }

  assert connection_lasting(0) == 20
  assert connection_lasting(0) == 40
  assert connection_lasting(0) == 60
  assert connection_lasting(0) == 60
  // A connection lasting `longest_ms` resets the wait.
  assert connection_lasting(100) == 20
  assert connection_lasting(0) == 40
  assert glomp.stop(client) == glomp.Idle
}

/// Values below the minimum are raised to it.
pub fn reconnect_is_clamped_test() {
  let events = process.new_subject()

  let client =
    test_client(broker.free_port(), events)
    |> glomp.reconnect(first_ms: -10, longest_ms: -20)
    |> start

  list.each([1, 2, 3], fn(attempt) {
    let assert glomp.Disconnected(glomp.Unreachable(ipv4:, ipv6:), wait) =
      next(events)

    assert ipv4 == network.ConnectionRefused
    assert ipv6 != None
    assert wait == 1 as int.to_string(attempt)
  })

  assert glomp.stop(client) == glomp.Idle
}

// ---------------------------------------------------------------------- TLS

/// An unknown certificate authority is rejected.
pub fn tls_refuses_an_unknown_authority_test() {
  let server = broker.start_tls()
  let events = process.new_subject()

  let client =
    glomp.new(host: "localhost", port: server.port)
    |> glomp.reconnect(first_ms: 5000, longest_ms: 5000)
    |> glomp.on_event(process.send(events, _))
    |> glomp.tls(trust: network.SystemCertificates)
    |> start

  assert broker.nobody_connects(server.listener, within: 1000)

  let assert glomp.Disconnected(glomp.Unreachable(ipv4:, ipv6:), 5000) =
    next(events)

  let assert network.TlsAlert(alert:, description:) = ipv4

  assert alert == network.UnknownCa
  assert description != ""
  assert ipv6 != None

  assert glomp.stop(client) == glomp.Idle
}

/// The certificate must match the host name.
pub fn tls_checks_the_host_name_test() {
  let server = broker.start_tls()
  let events = process.new_subject()

  let client =
    glomp.new(host: "127.0.0.1", port: server.port)
    |> glomp.reconnect(first_ms: 5000, longest_ms: 5000)
    |> glomp.on_event(process.send(events, _))
    |> glomp.tls(trust: network.CertificateFile(server.authority))
    |> start

  assert broker.nobody_connects(server.listener, within: 1000)

  let assert glomp.Disconnected(glomp.Unreachable(ipv4:, ipv6:), 5000) =
    next(events)

  let assert network.TlsAlert(alert:, description:) = ipv4

  assert alert == network.BadCertificate
  assert description != ""
  assert ipv6 != None

  assert glomp.stop(client) == glomp.Idle
}

/// A missing certificate file causes `Unreachable`.
pub fn tls_with_a_missing_certificate_file_test() {
  let server = broker.start_tls()
  let events = process.new_subject()

  let client =
    glomp.new(host: "localhost", port: server.port)
    |> glomp.reconnect(first_ms: 5000, longest_ms: 5000)
    |> glomp.on_event(process.send(events, _))
    |> glomp.tls(trust: network.CertificateFile("/nowhere/at/all.pem"))
    |> start

  let assert glomp.Disconnected(glomp.Unreachable(ipv4:, ipv6:), 5000) =
    next(events)

  let assert network.CertificateFileUnreadable(path:, reason:) = ipv4

  assert path == "/nowhere/at/all.pem"
  assert string.inspect(reason) == "Error(Enoent)"
  assert ipv6 != None
  assert glomp.stop(client) == glomp.Idle
}

/// A plain client on a TLS port fails to connect.
pub fn plain_to_a_tls_port_test() {
  let server = broker.start_tls()
  let events = process.new_subject()

  let client =
    test_client(server.port, events)
    |> glomp.connect_timeout(300)
    |> glomp.reconnect(first_ms: 5000, longest_ms: 5000)
    |> start

  assert broker.nobody_connects(server.listener, within: 1000)

  let assert glomp.Disconnected(reason, 5000) = next(events)

  assert list.contains([glomp.Closed, glomp.HandshakeTimedOut(300)], reason)
    || string.starts_with(string.inspect(reason), "Malformed")
    || string.starts_with(string.inspect(reason), "SocketFailed")
    as string.inspect(reason)

  assert glomp.stop(client) == glomp.Idle
}
