//// Runs only when `GLOMP_REAL_BROKERS` lists host:port:login:passcode entries.

import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/string
import glomp
import glomp/frame

@external(erlang, "broker_ffi", "env")
fn env(name: String) -> Result(String, Nil)

pub fn real_brokers_test() {
  case env("GLOMP_REAL_BROKERS") {
    Error(Nil) -> Nil

    Ok(brokers) ->
      string.split(brokers, " ")
      |> list.each(fn(broker) {
        let assert [host, port, login, passcode] = string.split(broker, ":")
        let assert Ok(port) = int.parse(port)

        round_trip(host, port, login, passcode)
      })
  }
}

/// Send, receive, acknowledge, then again inside a transaction.
fn round_trip(host: String, port: Int, login: String, passcode: String) -> Nil {
  let events = process.new_subject()
  let messages = process.new_subject()
  let queue = "/queue/glomp-test-" <> int.to_string(int.random(1_000_000_000))

  let assert Ok(started) =
    glomp.new(host:, port:)
    |> glomp.credentials(login:, passcode:)
    |> glomp.virtual_host("/")
    |> glomp.heart_beat(send_ms: 500, receive_ms: 500)
    |> glomp.on_event(process.send(events, _))
    |> glomp.with_subscription(
      glomp.acknowledged_subscription(
        id: "q",
        destination: queue,
        mode: glomp.Individual,
        handler: fn(message, delivery) {
          process.send(messages, #(message, delivery))
          glomp.Defer
        },
      ),
    )
    |> glomp.start

  let client = started.data

  assert glomp.await_connected(client, within: 10_000) == Ok(Nil)
    as { host <> ":" <> int.to_string(port) }

  let assert Ok(glomp.Connected(session)) = process.receive(events, 1000)
  assert session.version == frame.Stomp12
  assert process.receive(events, 5000) == Ok(glomp.Subscribed("q"))

  let body = <<"zero ":utf8, 0, " and line\nfeed":utf8>>
  let tricky = "a:b\\c\nd"

  assert glomp.send_confirmed(
      client,
      glomp.message(to: queue, body:)
        |> glomp.content_type("application/octet-stream")
        |> glomp.message_header(name: "x-tricky", value: tricky),
      within: 5000,
    )
    == Ok(Nil)

  let assert Ok(#(message, delivery)) = process.receive(messages, 5000)
  assert message.body == body
  assert message.subscription == "q"
  assert glomp.header(message, "x-tricky") == Ok(tricky)
  assert glomp.header(message, "content-type") == Ok("application/octet-stream")
  assert glomp.ack(client, delivery) == Ok(Nil)

  // Messages in a transaction arrive only after commit.
  let assert Ok(transaction) = glomp.begin(client)

  assert glomp.send(
      client,
      glomp.message(to: queue, body: <<"held":utf8>>)
        |> glomp.in_transaction(transaction),
    )
    == Ok(Nil)

  assert process.receive(messages, 300) == Error(Nil)
  assert glomp.commit(client, transaction, within: 5000) == Ok(Nil)

  let assert Ok(#(message, delivery)) = process.receive(messages, 5000)
  assert message.body == <<"held":utf8>>
  assert glomp.ack(client, delivery) == Ok(Nil)

  // Aborted messages never arrive.
  let assert Ok(transaction) = glomp.begin(client)

  assert glomp.send(
      client,
      glomp.message(to: queue, body: <<"dropped":utf8>>)
        |> glomp.in_transaction(transaction),
    )
    == Ok(Nil)

  assert glomp.abort(client, transaction) == Ok(Nil)
  assert process.receive(messages, 300) == Error(Nil)

  // Wait for several heart-beat intervals.
  process.sleep(2500)

  assert process.receive(events, 0) == Error(Nil)

  assert glomp.send_confirmed(
      client,
      glomp.message(to: queue, body: <<"still here":utf8>>),
      within: 5000,
    )
    == Ok(Nil)

  let assert Ok(#(message, delivery)) = process.receive(messages, 5000)
  assert message.body == <<"still here":utf8>>
  assert glomp.ack(client, delivery) == Ok(Nil)
  assert glomp.stop(client) == glomp.Graceful
}
