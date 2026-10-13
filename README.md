<h1 align="center">Glomp!</h1>
<h3 align="center">A STOMP 1.2 and 1.1 client for Gleam</h3>

```
gleam add glomp@1
```

## Quick start

Glomp is a client for STOMP, the text protocol many message brokers speak. Brokers are servers that pass messages between connected clients, like RabbitMQ or ActiveMQ.

This example subscribes to a queue, sends one message to it, and prints the message when it comes back.

```gleam
import gleam/erlang/process
import gleam/io
import gleam/otp/static_supervisor
import gleam/string
import glomp

pub fn main() {
  let greetings =
    glomp.subscription(
      id: "greetings",
      destination: "/queue/greetings",
      handler: fn(message) { io.println(string.inspect(message.body)) },
    )

  let client =
    glomp.new(host: "broker.example.com", port: 61_613)
    |> glomp.credentials(login: "user", passcode: "secret")
    |> glomp.with_subscription(greetings)

  let assert Ok(supervisor) =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(glomp.supervised(client))
    |> static_supervisor.start

  io.println("Supervisor started as " <> string.inspect(supervisor.pid))

  let assert Ok(Nil) = glomp.await_connected(client, within: 5000)

  let assert Ok(Nil) =
    glomp.send(
      client,
      glomp.message(to: "/queue/greetings", body: <<"Hello, Joe!">>),
    )

  process.sleep_forever()
}
```

The client runs under your supervisor. It connects in the background and it subscribes each time it connects. If the connection drops, the client reconnects by itself.

`await_connected` waits until the client is connected because sending will fail if the client is still connecting (not connected yet). You can skip calling await_connected if your program only ever subscribes to messages.

For everything else, such as sending, acknowledgements, transactions, TLS and events, see the API docs at https://hexdocs.pm/glomp.

## Licence

MIT
