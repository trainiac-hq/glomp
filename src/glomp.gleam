//// A STOMP client for talking to message brokers such as RabbitMQ,
//// ActiveMQ and Artemis. Start a client, and it connects in the background
//// and reconnects by itself if the connection drops. You can then send
//// messages and subscribe to destinations.
////
//// ```gleam
//// let assert Ok(started) =
////   glomp.new(host: "localhost", port: 61_613)
////   |> glomp.credentials(login: "guest", passcode: "guest")
////   |> glomp.start
//// let client = started.data
////
//// let assert Ok(Nil) = glomp.await_connected(client, within: 5000)
//// let assert Ok(Nil) =
////   glomp.send(client, glomp.message(to: "/queue/jobs", body: <<"hi">>))
//// ```

import gleam/bit_array
import gleam/bool
import gleam/dict.{type Dict}
import gleam/erlang/process.{type Name, type Pid, type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/otp/supervision
import gleam/result
import gleam/string
import glomp/frame.{type Frame, Frame}
import glomp/internal/bytes
import glomp/internal/clock
import glomp/internal/otp
import glomp/internal/socket
import glomp/internal/transport.{type Transport}
import glomp/network.{type NetworkError, type Trust}

/// The settings for a client, before it starts.
///
/// You get a `Builder` from `new`. Change it with functions such as
/// `credentials`, `tls` and `with_subscription`, then pass it to `start`
/// or `supervised`:
///
/// ```gleam
/// let builder =
///   glomp.new(host: "localhost", port: 61_613)
///   |> glomp.credentials(login: "guest", passcode: "guest")
///
/// let assert Ok(started) = glomp.start(builder)
/// ```
///
/// You can keep the `Builder` after the client starts. Pass it to `client`
/// to get the client's handle at any time.
pub opaque type Builder {
  Builder(
    host: String,
    port: Int,
    virtual_host: String,
    login: Option(#(String, String)),
    connect_headers: List(#(String, String)),
    security: socket.Security,
    heart_beat_send_ms: Int,
    heart_beat_receive_ms: Int,
    connect_timeout_ms: Int,
    limits: frame.Limits,
    retry_first_ms: Int,
    retry_longest_ms: Int,
    subscriptions: List(Subscription),
    on_event: Option(fn(Event) -> Nil),
    name: Name(Signal),
  )
}

/// Create a client for the broker at `host` and `port`.
///
/// Call `new` once per client, when your program starts. Each call creates
/// an atom, which is a name the Erlang VM keeps forever. The VM can only
/// hold a fixed number of atoms, so calling `new` over and over while your
/// program runs will eventually crash the VM.
pub fn new(host host: String, port port: Int) -> Builder {
  Builder(
    host:,
    port:,
    virtual_host: host,
    login: None,
    connect_headers: [],
    security: socket.Plain,
    heart_beat_send_ms: 30_000,
    heart_beat_receive_ms: 30_000,
    connect_timeout_ms: 30_000,
    limits: frame.default_limits,
    retry_first_ms: 1000,
    retry_longest_ms: 60_000,
    subscriptions: [],
    on_event: None,
    name: process.new_name("glomp"),
  )
}

/// Set the `login` and `passcode` headers.
///
/// Each STOMP header is one line of text, so a line feed ends it. Glomp
/// sends the CONNECT frame exactly as written, because STOMP 1.0 brokers
/// expect that. If `login` or `passcode` contains a line feed, the header
/// ends there and the broker reads the rest as a separate header.
pub fn credentials(
  builder: Builder,
  login login: String,
  passcode passcode: String,
) -> Builder {
  Builder(..builder, login: Some(#(login, passcode)))
}

/// Set the `host` header, which picks a virtual host on the broker. It
/// defaults to the broker's host name. RabbitMQ calls this a vhost.
pub fn virtual_host(builder: Builder, virtual_host: String) -> Builder {
  Builder(..builder, virtual_host:)
}

/// Set the `client-id` header. ActiveMQ and Artemis use it to keep durable
/// subscriptions across reconnects. They allow only one connection per id
/// at a time, so a second client with the same id is refused.
pub fn client_id(builder: Builder, id: String) -> Builder {
  connect_header(builder, "client-id", id)
}

/// Add a header to the CONNECT frame.
pub fn connect_header(
  builder: Builder,
  name name: String,
  value value: String,
) -> Builder {
  Builder(
    ..builder,
    connect_headers: list.append(builder.connect_headers, [#(name, value)]),
  )
}

/// Connect over TLS. Pass the broker's TLS port to `new`. Many brokers
/// use port 61614 for this.
pub fn tls(builder: Builder, trust trust: Trust) -> Builder {
  Builder(..builder, security: socket.Tls(trust:))
}

/// Set the heart-beat intervals, in milliseconds. A heart-beat is a small
/// keep-alive that shows the connection is still working.
///
/// `send_ms` is how often the client can send one. `receive_ms` is how
/// often it wants to receive one. Setting either to 0 turns that direction
/// off.
///
/// For each direction, the client and broker use the larger of their two
/// numbers. If either side says 0, that direction is off. You can see the
/// agreed values in `Session`.
///
/// The client sends at half the agreed interval, so a slow network still
/// delivers one in time. If two whole intervals pass with nothing from the
/// broker, the client disconnects with `HeartBeatMissed`.
pub fn heart_beat(
  builder: Builder,
  send_ms send_ms: Int,
  receive_ms receive_ms: Int,
) -> Builder {
  Builder(
    ..builder,
    heart_beat_send_ms: int.max(send_ms, 0),
    heart_beat_receive_ms: int.max(receive_ms, 0),
  )
}

/// Set the time limit for one connection attempt, in milliseconds.
pub fn connect_timeout(builder: Builder, milliseconds: Int) -> Builder {
  Builder(..builder, connect_timeout_ms: milliseconds)
}

/// Set the largest frame the client will accept. This stops one huge frame
/// from using up memory. If a larger frame arrives, the client closes the
/// connection with `Malformed`.
pub fn limits(builder: Builder, limits: frame.Limits) -> Builder {
  Builder(..builder, limits:)
}

/// Set how long to wait before reconnecting, in milliseconds.
///
/// The wait starts at `first_ms` and doubles after each failure, up to
/// `longest_ms`. This keeps a broker that is down from being flooded with
/// attempts. Once a connection stays up for `longest_ms`, the wait goes
/// back to `first_ms`.
pub fn reconnect(
  builder: Builder,
  first_ms first_ms: Int,
  longest_ms longest_ms: Int,
) -> Builder {
  let first_ms = int.max(first_ms, 1)

  Builder(
    ..builder,
    retry_first_ms: first_ms,
    retry_longest_ms: int.max(longest_ms, first_ms),
  )
}

/// Add a subscription before the client starts.
///
/// ```gleam
/// glomp.new(host:, port:)
/// |> glomp.with_subscription(
///   glomp.subscription(id: "jobs", destination: "/queue/jobs", handler:),
/// )
/// ```
///
/// The client subscribes on every connection, in the order you added them.
///
/// To add one to a client that is already running, use `subscribe`.
pub fn with_subscription(
  builder: Builder,
  subscription: Subscription,
) -> Builder {
  Builder(
    ..builder,
    subscriptions: list.append(builder.subscriptions, [subscription]),
  )
}

/// Set a function to receive `Event`s. It runs in the handler process,
/// in the same order as your message handlers.
pub fn on_event(builder: Builder, handler: fn(Event) -> Nil) -> Builder {
  Builder(..builder, on_event: Some(handler))
}

// ------------------------------------------------------------ subscriptions

/// A subscription asks the broker to send you the messages from one
/// destination, such as a queue. It has an id, the destination and the
/// handler that receives each message.
///
/// Create one with `subscription` or `acknowledged_subscription`. Add it
/// with `with_subscription` before the client starts, or with `subscribe`
/// while it runs. The client subscribes again on every new connection,
/// until you pass its id to `unsubscribe`.
pub opaque type Subscription {
  Subscription(
    id: String,
    destination: String,
    headers: List(#(String, String)),
    ack: Option(AckMode),
    handler: Handler,
  )
}

type Handler {
  Automatic(handle: fn(Message) -> Nil)
  Acknowledged(handle: fn(Message, Delivery) -> Verdict)
}

/// How many messages the broker marks as done when you acknowledge one.
///
/// To acknowledge a message is to tell the broker you have handled it, so
/// the broker does not send it again.
pub type AckMode {
  /// An acknowledgement covers this message and all earlier ones. This is
  /// STOMP's `client` mode.
  Cumulative
  /// An acknowledgement covers only this message. This is STOMP's
  /// `client-individual` mode.
  Individual
}

/// What your handler tells the broker about a message. The handler returns
/// one of these for each message it gets.
pub type Verdict {
  /// The message was handled. The client sends ACK to the broker.
  Ack
  /// The message was not handled. The client sends NACK, and the broker
  /// decides what to do with the message.
  Nack
  /// You will decide later. Keep the `Delivery` and call `ack` or `nack`
  /// when you're done.
  Defer
}

/// Create a plain subscription. This is STOMP's `auto` mode.
///
/// A plain subscription does not need to acknowledge messages. A message
/// is only lost if the connection dies while you're handling it. To keep
/// those messages, use `acknowledged_subscription`.
///
/// Give each subscription in a client its own `id`. The broker puts the id
/// on every message, so the client knows which handler to call. `start`
/// fails if two subscriptions share an id.
pub fn subscription(
  id id: String,
  destination destination: String,
  handler handler: fn(Message) -> Nil,
) -> Subscription {
  Subscription(
    id:,
    destination:,
    headers: [],
    ack: None,
    handler: Automatic(handler),
  )
}

/// Create a subscription where you acknowledge each message.
///
/// The handler returns a `Verdict`. To decide later, return `Defer` and
/// pass the `Delivery` to `ack` or `nack`.
///
/// After a reconnect, the broker sends again any message you had not
/// acknowledged yet. This means a handler can see the same message twice.
pub fn acknowledged_subscription(
  id id: String,
  destination destination: String,
  mode mode: AckMode,
  handler handler: fn(Message, Delivery) -> Verdict,
) -> Subscription {
  Subscription(
    id:,
    destination:,
    headers: [],
    ack: Some(mode),
    handler: Acknowledged(handler),
  )
}

/// Add a header to the SUBSCRIBE frame, such as
/// `activemq.subscriptionName`, `selector` or `prefetch-count`.
pub fn subscription_header(
  subscription: Subscription,
  name name: String,
  value value: String,
) -> Subscription {
  Subscription(
    ..subscription,
    headers: list.append(subscription.headers, [#(name, value)]),
  )
}

// ----------------------------------------------------------------- messages

/// A message from the broker.
///
/// Your subscription's handler gets each `Message` as it arrives. Read its
/// fields directly, or use `header` to find one header. A `Message` is a
/// plain value, so it stays valid after the connection ends.
pub type Message {
  Message(
    /// The destination it was sent to.
    destination: String,
    /// The broker's `message-id`.
    id: String,
    /// The id of the subscription it arrived on.
    subscription: String,
    /// All headers, in the order the broker sent them.
    headers: List(#(String, String)),
    /// The body exactly as the broker sent it.
    body: BitArray,
  )
}

/// Get the first header called `name`.
pub fn header(message: Message, name: String) -> Result(String, Nil) {
  list.key_find(message.headers, name)
}

/// A token for acknowledging one message.
///
/// The handler of an `acknowledged_subscription` gets a `Delivery` with
/// each message. To acknowledge the message later, return `Defer` from the
/// handler, keep the `Delivery`, and pass it to `ack` or `nack` when the
/// work is done:
///
/// ```gleam
/// glomp.acknowledged_subscription(
///   id: "jobs",
///   destination: "/queue/jobs",
///   mode: glomp.Individual,
///   handler: fn(message, delivery) {
///     process.send(worker, #(message, delivery))
///     glomp.Defer
///   },
/// )
///
/// // Later, when the worker has finished
/// let assert Ok(Nil) = glomp.ack(client, delivery)
/// ```
///
/// A `Delivery` only works on the connection its message arrived on. When
/// that connection ends, `ack` and `nack` return `Expired`, and the broker
/// sends the message again on the next connection.
pub opaque type Delivery {
  Delivery(connection_id: Int, headers: List(#(String, String)))
}

/// A message to send.
///
/// Create one with `message`, change it with functions such as
/// `content_type`, then pass it to `send` or `send_confirmed`:
///
/// ```gleam
/// let outbound =
///   glomp.message(to: "/queue/jobs", body: <<"{}">>)
///   |> glomp.content_type("application/json")
///
/// let assert Ok(Nil) = glomp.send(client, outbound)
/// ```
///
/// An `Outbound` is a plain value, so you can send it more than once. One
/// made with `in_transaction` only works while its transaction does.
pub opaque type Outbound {
  Outbound(
    destination: String,
    body: BitArray,
    content_type: Option(String),
    headers: List(#(String, String)),
    content_length: Bool,
    transaction: Option(Transaction),
  )
}

/// Create a message for `destination`.
pub fn message(to destination: String, body body: BitArray) -> Outbound {
  Outbound(
    destination:,
    body:,
    content_type: None,
    headers: [],
    content_length: True,
    transaction: None,
  )
}

/// Set the `content-type` header, such as `application/json`.
pub fn content_type(outbound: Outbound, content_type: String) -> Outbound {
  Outbound(..outbound, content_type: Some(content_type))
}

/// Add a header to the SEND frame.
pub fn message_header(
  outbound: Outbound,
  name name: String,
  value value: String,
) -> Outbound {
  Outbound(..outbound, headers: list.append(outbound.headers, [#(name, value)]))
}

/// Leave out the `content-length` header. ActiveMQ treats a message with
/// this header as bytes and a message without it as text.
///
/// A body that contains a NUL byte still gets the header. A NUL byte marks
/// the end of a frame, so the broker needs the length to read past it.
pub fn without_content_length(outbound: Outbound) -> Outbound {
  Outbound(..outbound, content_length: False)
}

/// Send the message as part of a transaction. The broker only delivers it
/// when you call `commit`.
pub fn in_transaction(
  outbound: Outbound,
  transaction: Transaction,
) -> Outbound {
  Outbound(..outbound, transaction: Some(transaction))
}

/// A group of sends and acknowledgements that the broker applies all at
/// once, when you commit.
///
/// You get a `Transaction` from `begin`. Add messages with
/// `in_transaction`, and acknowledgements with `ack_in` and `nack_in`.
///
/// Then call `commit` to apply them, or `abort` to throw them away:
///
/// ```gleam
/// let assert Ok(transaction) = glomp.begin(client)
///
/// let outbound =
///   glomp.message(to: "/queue/jobs", body: <<"hi">>)
///   |> glomp.in_transaction(transaction)
/// let assert Ok(Nil) = glomp.send(client, outbound)
///
/// let assert Ok(Nil) = glomp.commit(client, transaction, within: 5000)
/// ```
///
/// A `Transaction` only works on the connection that started it, because
/// the broker aborts open transactions when that connection ends. After
/// that, using it returns `Expired`.
pub opaque type Transaction {
  Transaction(connection_id: Int, id: String)
}

// ------------------------------------------------------------- what happens

/// Details about a connection, which the client and broker agreed on when
/// they connected.
///
/// You get a `Session` in the `Connected` event, which your `on_event`
/// function receives. It describes that one connection. The next
/// connection brings a new `Session`.
pub type Session {
  Session(
    version: frame.Version,
    /// The broker's `server` header.
    server: Option(String),
    /// The broker's `session` header.
    id: Option(String),
    /// How often the client sends a heart-beat. If it is 0, the client does
    /// not send them.
    heart_beat_send_ms: Int,
    /// How often the broker sends a heart-beat. If it is 0, the broker does
    /// not send them.
    heart_beat_receive_ms: Int,
    /// All headers from the broker's CONNECTED frame, which is its reply
    /// to CONNECT.
    headers: List(#(String, String)),
  )
}

/// News about the connection, such as connecting or losing it. Your
/// `on_event` function receives each one.
pub type Event {
  /// The client connected and asked the broker for every subscription.
  Connected(session: Session)
  /// The broker confirmed this subscription.
  Subscribed(id: String)
  /// The connection ended, or an attempt to connect failed. The client
  /// tries again after `retry_in_ms`.
  Disconnected(reason: Disconnect, retry_in_ms: Int)
  /// The client skipped a frame because it could not use it.
  Dropped(frame: Frame, reason: DropReason)
}

/// The reason a connection ended, or why the client could not connect.
pub type Disconnect {
  /// Connecting over IPv4 failed, and so did IPv6. The client only tries
  /// IPv6 if there is time left.
  Unreachable(ipv4: NetworkError, ipv6: Option(NetworkError))
  /// The process that makes the connection crashed.
  ConnectorCrashed(reason: process.ExitReason)
  /// The broker did not answer CONNECT within the connect timeout.
  HandshakeTimedOut(waited_ms: Int)
  /// The broker answered CONNECT with an ERROR frame. This usually means
  /// the credentials are wrong or another client is using the client id.
  Refused(message: String, frame: Frame)
  /// The broker chose a STOMP version that glomp does not support. Glomp
  /// supports 1.1 and 1.2.
  UnsupportedVersion(version: String)
  /// The broker sent a frame that is not valid at that moment. For example,
  /// it answered CONNECT with a MESSAGE, or sent a second CONNECTED.
  UnexpectedFrame(frame: Frame)
  /// The broker sent a frame that is invalid or too large.
  Malformed(error: frame.DecodeError)
  /// The broker sent an ERROR frame.
  BrokerError(message: String, frame: Frame)
  /// The broker closed the connection.
  Closed
  /// Reading or writing the socket failed.
  SocketFailed(error: NetworkError)
  /// Two heart-beat intervals passed without anything from the broker.
  HeartBeatMissed(silent_ms: Int)
}

/// The reason the client ignored a frame from the broker.
pub type DropReason {
  /// The frame is missing a header it needs.
  MissingHeader(name: String)
  /// A MESSAGE arrived for a subscription the client does not know. This
  /// usually happens just after you unsubscribe.
  UnknownSubscription(id: String)
  /// A RECEIPT arrived with an id the client does not know. This usually
  /// means it came after the wait ended with `NoReceipt`.
  UnknownReceipt(id: String)
}

/// The reason a request such as `send` or `ack` failed.
pub type Error {
  /// The client is not connected. It does not queue requests to send
  /// later, so the request was not sent.
  NotConnected
  /// The connection dropped while the frame was being written. The broker
  /// may or may not have received it.
  ConnectionLost(reason: Disconnect)
  /// The `Delivery` or `Transaction` came from an earlier connection, so
  /// the broker no longer knows it.
  Expired
  /// The broker's receipt did not arrive in time. The broker may still
  /// have done what you asked.
  NoReceipt(waited_ms: Int)
}

/// `subscribe` returns this when the client already has a subscription
/// with this id.
pub type AlreadySubscribed {
  AlreadySubscribed(id: String)
}

/// `unsubscribe` returns this when the client does not have a
/// subscription with this id.
pub type NotSubscribed {
  NotSubscribed(id: String)
}

/// What happened to the connection when you called `stop`.
///
/// `stop` returns this once the client has exited. You can log it or
/// ignore it, because the client is gone either way.
pub type Stopped {
  /// The broker confirmed the DISCONNECT.
  Graceful
  /// The broker did not confirm the DISCONNECT within two seconds.
  Unconfirmed
  /// The connection failed before the broker could confirm.
  Abrupt(reason: Disconnect)
  /// The client was not connected.
  Idle
}

// ---------------------------------------------------------------- the client

/// A handle to a running client. You can use it from any process.
///
/// You get a `Client` from `start`'s result, or from `client` with the
/// same `Builder`. Every function that talks to the broker takes it, such
/// as `send`, `subscribe`, `ack` and `stop`.
///
/// The handle finds the client by name, so it keeps working after a
/// supervisor restarts the client. While the client is stopped, calls
/// with it crash.
pub opaque type Client {
  Client(subject: Subject(Signal))
}

/// Messages sent to the client process.
type Signal {
  Subscribing(
    subscription: Subscription,
    reply: Subject(Result(Nil, AlreadySubscribed)),
  )
  Unsubscribing(id: String, reply: Subject(Result(Nil, NotSubscribed)))
  Sending(
    outbound: Outbound,
    confirm_within_ms: Option(Int),
    reply: Subject(Result(Nil, Error)),
  )
  Acknowledging(
    delivery: Delivery,
    settlement: Acknowledgement,
    transaction: Option(Transaction),
    reply: Subject(Result(Nil, Error)),
  )
  Beginning(reply: Subject(Result(Transaction, Error)))
  Committing(
    transaction: Transaction,
    within_ms: Int,
    reply: Subject(Result(Nil, Error)),
  )
  Aborting(transaction: Transaction, reply: Subject(Result(Nil, Error)))
  Awaiting(reply: Subject(Nil), until: Int)
  Stopping(reply: Subject(Stopped))
  StartConnecting
  ConnectFinished(result: Result(Established, Disconnect))
  SocketMessage(incoming: socket.Incoming)
  HeartBeatDue(connection_id: Int)
  HeartBeatCheckDue(connection_id: Int)
  ReceiptOverdue(receipt: String, waited_ms: Int)
  HandlerFinished(
    connection_id: Int,
    acknowledge: Option(#(Delivery, Acknowledgement)),
  )
  Exited(exit: process.ExitMessage)
}

type Acknowledgement {
  SendAck
  SendNack
}

/// A transport that has received the CONNECTED frame.
type Established {
  Established(transport: Transport, session: Session)
}

/// Start a client linked to the caller. The client stops when the caller
/// exits.
///
/// This returns as soon as the client is running, and the client connects
/// after that. To wait for the connection, use `await_connected`.
///
/// It fails if two subscriptions share an id, or if this client is already
/// running.
pub fn start(
  builder: Builder,
) -> Result(actor.Started(Client), actor.StartError) {
  use Nil <- result.try(
    unique_ids(builder.subscriptions, [])
    |> result.map_error(actor.InitFailed),
  )

  let parent = process.self()

  use started <- result.map(
    actor.new_with_initialiser(5000, initialise(builder, parent, _))
    |> actor.on_message(handle)
    |> actor.named(builder.name)
    |> actor.start,
  )

  actor.Started(pid: started.pid, data: client(builder))
}

/// A child specification for a supervisor. Use `client` to get the handle.
///
/// When the supervisor shuts it down, the client sends DISCONNECT first.
pub fn supervised(builder: Builder) -> supervision.ChildSpecification(Client) {
  supervision.worker(fn() { start(builder) })
}

/// Get the handle for a client. You can get it before the client starts,
/// and it keeps working after restarts.
///
/// The handle finds the client process by name. If you call a function
/// while the client is stopped, the call crashes, because no process has
/// that name.
pub fn client(builder: Builder) -> Client {
  Client(process.named_subject(builder.name))
}

/// Wait up to `within_ms` for the client to connect.
///
/// `Error` means the time ran out. The client keeps trying to connect in
/// the background.
pub fn await_connected(
  client: Client,
  within within_ms: Int,
) -> Result(Nil, Nil) {
  let reply = process.new_subject()

  process.send(client.subject, Awaiting(reply, clock.now_ms() + within_ms))
  process.receive(reply, within_ms)
}

/// Send a message. `Ok` means the message was written to the socket. It
/// does not mean the broker has received it.
///
/// Errors: `NotConnected`, `ConnectionLost`, `Expired`.
pub fn send(client: Client, outbound: Outbound) -> Result(Nil, Error) {
  process.call_forever(client.subject, Sending(outbound, None, _))
}

/// Send a message and wait for the broker's receipt. A receipt is the
/// broker's reply saying it has received the frame.
///
/// Errors: the same as `send`, plus `NoReceipt`.
pub fn send_confirmed(
  client: Client,
  outbound: Outbound,
  within within_ms: Int,
) -> Result(Nil, Error) {
  process.call_forever(client.subject, Sending(outbound, Some(within_ms), _))
}

/// Add a subscription to a running client. If the client is connected, it
/// subscribes straight away. It also subscribes on every later connection.
///
/// `Ok` means the client added it. The `Subscribed` event comes later,
/// when the broker confirms it.
pub fn subscribe(
  client: Client,
  subscription: Subscription,
) -> Result(Nil, AlreadySubscribed) {
  process.call_forever(client.subject, Subscribing(subscription, _))
}

/// Remove a subscription. Messages the broker had already sent for it
/// arrive as `Dropped` events.
pub fn unsubscribe(client: Client, id: String) -> Result(Nil, NotSubscribed) {
  process.call_forever(client.subject, Unsubscribing(id, _))
}

/// Tell the broker you handled a deferred message, by sending ACK.
///
/// Errors: `Expired`, `ConnectionLost`.
pub fn ack(client: Client, delivery: Delivery) -> Result(Nil, Error) {
  process.call_forever(client.subject, Acknowledging(delivery, SendAck, None, _))
}

/// Tell the broker you did not handle a deferred message, by sending NACK.
///
/// Errors: `Expired`, `ConnectionLost`.
pub fn nack(client: Client, delivery: Delivery) -> Result(Nil, Error) {
  process.call_forever(client.subject, Acknowledging(
    delivery,
    SendNack,
    None,
    _,
  ))
}

/// Like `ack`, but as part of a transaction.
pub fn ack_in(
  client: Client,
  delivery: Delivery,
  transaction: Transaction,
) -> Result(Nil, Error) {
  process.call_forever(client.subject, Acknowledging(
    delivery,
    SendAck,
    Some(transaction),
    _,
  ))
}

/// Like `nack`, but as part of a transaction.
pub fn nack_in(
  client: Client,
  delivery: Delivery,
  transaction: Transaction,
) -> Result(Nil, Error) {
  process.call_forever(client.subject, Acknowledging(
    delivery,
    SendNack,
    Some(transaction),
    _,
  ))
}

/// Begin a transaction. The broker holds back what you add to it until you
/// call `commit`. Add to it with `in_transaction`, `ack_in` and `nack_in`.
///
/// Errors: `NotConnected`, `ConnectionLost`.
pub fn begin(client: Client) -> Result(Transaction, Error) {
  process.call_forever(client.subject, Beginning)
}

/// Commit a transaction and wait for the broker's receipt.
///
/// Errors: `Expired`, `ConnectionLost`, `NoReceipt`.
pub fn commit(
  client: Client,
  transaction: Transaction,
  within within_ms: Int,
) -> Result(Nil, Error) {
  process.call_forever(client.subject, Committing(transaction, within_ms, _))
}

/// Abort a transaction.
///
/// Errors: `Expired`, `ConnectionLost`.
pub fn abort(client: Client, transaction: Transaction) -> Result(Nil, Error) {
  process.call_forever(client.subject, Aborting(transaction, _))
}

/// Stop a client started with `start`.
///
/// The client sends DISCONNECT and waits up to two seconds for the receipt,
/// then it closes the socket and exits. Any handlers still running are
/// killed.
///
/// A supervised client comes straight back if you stop it this way. The
/// client disconnects and exits, its supervisor sees that it has exited,
/// and the supervisor starts a new client that connects again. To stop a
/// supervised client for good, stop its supervisor.
pub fn stop(client: Client) -> Stopped {
  let assert Ok(pid) = process.subject_owner(client.subject)
    as "glomp: the client is not running"

  let monitor = process.monitor(pid)
  let stopped = process.call_forever(client.subject, Stopping)

  let exit_reason =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(down) { down.reason })
    |> process.selector_receive_forever

  // The client has exited, so its name is free.
  case exit_reason {
    process.Normal -> stopped
    process.Killed -> otp.exit(process.Killed)
    process.Abnormal(reason:) -> otp.exit(reason)
  }
}

// ------------------------------------------------------------------- state

type State {
  State(
    config: Builder,
    self: Subject(Signal),
    handlers: Subject(HandlerMessage),
    subscriptions: List(Subscription),
    status: Status,
    /// Goes up by one on each connection. It tells which connection a
    /// value came from.
    connection_id: Int,
    /// How many attempts in a row failed or ended quickly.
    failures: Int,
    /// `await_connected` callers, each with its deadline.
    connection_waiters: List(#(Subject(Nil), Int)),
    /// The process connecting, while `status` is `Connecting`.
    connector: Option(Pid),
    /// The process that runs handlers.
    handler_process: Pid,
    /// The process that started this one.
    parent: Pid,
    /// The last number used for a receipt or transaction id.
    serial: Int,
  )
}

type Status {
  /// Waiting for the retry timer.
  WaitingToRetry
  /// A process is connecting and will send `ConnectFinished`.
  Connecting
  Open(Connection)
}

type Connection {
  Connection(
    transport: Transport,
    session: Session,
    since: Int,
    /// Callers waiting for a receipt, by receipt id.
    pending_receipts: Dict(String, Subject(Result(Nil, Error))),
    /// SUBSCRIBEs waiting for confirmation, as subscription id by receipt id.
    pending_subscribes: Dict(String, String),
    /// Messages whose handlers are still running.
    unhandled: Int,
    /// Whether the next packet has been requested.
    packet_requested: Bool,
    last_received_at: Int,
  )
}

/// Messages sent to the handler process.
type HandlerMessage {
  Register(id: String, handler: Handler)
  Forget(id: String)
  Deliver(message: Message, delivery: Delivery)
  Tell(event: Event)
}

const max_unhandled = 64

const disconnect_timeout_ms = 2000

fn initialise(
  config: Builder,
  parent: Pid,
  self: Subject(Signal),
) -> Result(actor.Initialised(State, Signal, Client), String) {
  // Trap exits to send DISCONNECT before shutting down.
  process.trap_exits(True)

  use handlers <- result.map(
    start_handlers(config, self)
    |> result.map_error(fn(error) {
      "glomp: the handler process did not start: " <> string.inspect(error)
    }),
  )

  list.each(config.subscriptions, fn(subscription) {
    process.send(handlers.data, Register(subscription.id, subscription.handler))
  })

  process.send(self, StartConnecting)

  let selector =
    process.new_selector()
    |> process.select(self)
    |> transport.select(SocketMessage)
    |> process.select_trapped_exits(Exited)

  actor.initialised(State(
    config:,
    self:,
    handlers: handlers.data,
    subscriptions: config.subscriptions,
    status: WaitingToRetry,
    connection_id: 0,
    failures: 0,
    connection_waiters: [],
    connector: None,
    handler_process: handlers.pid,
    parent:,
    serial: 0,
  ))
  |> actor.selecting(selector)
  |> actor.returning(Client(self))
}

fn unique_ids(
  subscriptions: List(Subscription),
  seen: List(String),
) -> Result(Nil, String) {
  case subscriptions {
    [] -> Ok(Nil)

    [first, ..rest] ->
      case list.contains(seen, first.id) {
        True -> Error("glomp: two subscriptions have the id " <> first.id)
        False -> unique_ids(rest, [first.id, ..seen])
      }
  }
}

// ------------------------------------------------------- the handler process

/// Start the linked process that runs handlers and `on_event`.
fn start_handlers(
  config: Builder,
  client: Subject(Signal),
) -> Result(actor.Started(Subject(HandlerMessage)), actor.StartError) {
  actor.new(Handlers(client:, on_event: config.on_event, handlers: dict.new()))
  |> actor.on_message(run_handler)
  |> actor.start
}

type Handlers {
  Handlers(
    client: Subject(Signal),
    on_event: Option(fn(Event) -> Nil),
    handlers: Dict(String, Handler),
  )
}

fn run_handler(
  state: Handlers,
  message: HandlerMessage,
) -> actor.Next(Handlers, HandlerMessage) {
  case message {
    Register(id:, handler:) -> {
      let handlers = dict.insert(state.handlers, id, handler)

      actor.continue(Handlers(..state, handlers:))
    }

    Forget(id:) -> {
      let handlers = dict.delete(state.handlers, id)

      actor.continue(Handlers(..state, handlers:))
    }

    Tell(event:) -> {
      case state.on_event {
        Some(tell) -> tell(event)
        None -> Nil
      }

      actor.continue(state)
    }

    Deliver(message:, delivery:) -> {
      let acknowledge = case dict.get(state.handlers, message.subscription) {
        // The subscription was removed before this message was handled.
        Error(Nil) -> None

        Ok(Automatic(handle:)) -> {
          handle(message)
          None
        }

        Ok(Acknowledged(handle:)) ->
          case handle(message, delivery) {
            Ack -> Some(#(delivery, SendAck))
            Nack -> Some(#(delivery, SendNack))
            Defer -> None
          }
      }

      process.send(
        state.client,
        HandlerFinished(delivery.connection_id, acknowledge),
      )

      actor.continue(state)
    }
  }
}

fn tell(state: State, event: Event) -> Nil {
  process.send(state.handlers, Tell(event))
}

// ------------------------------------------------------------------ helpers

fn set_connection(state: State, connection: Connection) -> State {
  State(..state, status: Open(connection))
}

/// Run `then` with the connection, if connected.
fn with_connection(state: State, then: fn(Connection) -> State) -> State {
  case state.status {
    WaitingToRetry | Connecting -> state
    Open(connection) -> then(connection)
  }
}

/// Run `then` if `connection_id` is the current connection.
fn with_current_connection(
  state: State,
  connection_id: Int,
  then: fn(Connection) -> State,
) -> State {
  use <- bool.guard(connection_id != state.connection_id, state)

  with_connection(state, then)
}

/// Write a frame. On failure, close and reconnect.
fn try_write(
  state: State,
  connection: Connection,
  frame: Frame,
  then: fn() -> State,
) -> State {
  case write(connection, frame) {
    Ok(Nil) -> then()
    Error(reason) -> fail_connection(state, connection, reason)
  }
}

/// Write a frame. On failure, also reply with the error.
fn try_write_for(
  state: State,
  connection: Connection,
  frame: Frame,
  reply: Subject(Result(answer, Error)),
  then: fn() -> State,
) -> State {
  case write(connection, frame) {
    Ok(Nil) -> then()

    Error(reason) -> {
      process.send(reply, Error(ConnectionLost(reason)))
      fail_connection(state, connection, reason)
    }
  }
}

fn reply_error(
  state: State,
  reply: Subject(Result(answer, error)),
  error: error,
) -> State {
  process.send(reply, Error(error))
  state
}

/// Record that the transport received bytes.
fn with_received(connection: Connection, transport: Transport) -> Connection {
  Connection(
    ..connection,
    transport:,
    packet_requested: False,
    last_received_at: clock.now_ms(),
  )
}

/// Reply `Ok` to the caller waiting on receipt `id`.
fn resolve_receipt(
  waiting: Dict(String, Subject(Result(Nil, Error))),
  id: String,
) -> Result(Dict(String, Subject(Result(Nil, Error))), Nil) {
  use reply <- result.map(dict.get(waiting, id))

  process.send(reply, Ok(Nil))
  dict.delete(waiting, id)
}

// ---------------------------------------------------------------- signals

fn handle(state: State, signal: Signal) -> actor.Next(State, Signal) {
  case signal {
    StartConnecting -> actor.continue(start_connecting(state))

    ConnectFinished(result: Ok(established)) ->
      actor.continue(connected(State(..state, connector: None), established))

    ConnectFinished(result: Error(reason)) ->
      actor.continue(schedule_reconnect(State(..state, connector: None), reason))

    SocketMessage(incoming:) -> actor.continue(socket_message(state, incoming))

    HeartBeatDue(connection_id:) ->
      actor.continue(send_heart_beat(state, connection_id))

    HeartBeatCheckDue(connection_id:) ->
      actor.continue(check_heart_beat(state, connection_id))

    HandlerFinished(connection_id:, acknowledge:) ->
      actor.continue(handler_finished(state, connection_id, acknowledge))

    ReceiptOverdue(receipt:, waited_ms:) ->
      actor.continue(receipt_overdue(state, receipt, waited_ms))

    Sending(outbound:, confirm_within_ms:, reply:) ->
      actor.continue(write_for_caller(
        state,
        outbound.transaction,
        outbound_frame(outbound),
        confirm_within_ms,
        reply,
      ))

    Acknowledging(delivery:, settlement:, transaction:, reply:) ->
      actor.continue(acknowledging(
        state,
        delivery,
        settlement,
        transaction,
        reply,
      ))

    Beginning(reply:) -> actor.continue(begin_now(state, reply))

    Committing(transaction:, within_ms:, reply:) ->
      actor.continue(write_for_caller(
        state,
        Some(transaction),
        Frame(frame.Commit, [#("transaction", transaction.id)], <<>>),
        Some(within_ms),
        reply,
      ))

    Aborting(transaction:, reply:) ->
      actor.continue(write_for_caller(
        state,
        Some(transaction),
        Frame(frame.Abort, [#("transaction", transaction.id)], <<>>),
        None,
        reply,
      ))

    Subscribing(subscription:, reply:) ->
      actor.continue(subscribe_now(state, subscription, reply))

    Unsubscribing(id:, reply:) ->
      actor.continue(unsubscribe_now(state, id, reply))

    Awaiting(reply:, until:) ->
      actor.continue(connection_waiters(state, reply, until))

    Stopping(reply:) -> stop_now(state, reply)
    Exited(exit:) -> exited(state, exit)
  }
}

fn connection_waiters(state: State, reply: Subject(Nil), until: Int) -> State {
  case state.status {
    Open(connection) -> {
      process.send(reply, Nil)
      set_connection(state, connection)
    }

    WaitingToRetry | Connecting ->
      State(..state, connection_waiters: [
        #(reply, until),
        ..state.connection_waiters
      ])
  }
}

fn stop_now(
  state: State,
  reply: Subject(Stopped),
) -> actor.Next(State, Signal) {
  process.send(reply, shut_down(state))
  actor.stop()
}

/// Handle the exit of a linked process.
fn exited(
  state: State,
  exit: process.ExitMessage,
) -> actor.Next(State, Signal) {
  let from_parent = exit.pid == state.parent
  let from_handlers = exit.pid == state.handler_process
  let from_connector = Some(exit.pid) == state.connector

  // The parent is stopping this process, or a handler crashed.
  use <- bool.lazy_guard(from_parent || from_handlers, fn() {
    case exit.reason {
      process.Normal -> {
        shut_down(state)
        actor.stop()
      }

      // Killed, so exit without DISCONNECT.
      process.Killed -> otp.exit(process.Killed)

      process.Abnormal(reason:) -> {
        shut_down(state)
        otp.exit(reason)
      }
    }
  })

  // Other exits are ignored. A connector exit schedules a reconnect.
  use <- bool.guard(!from_connector, actor.continue(state))

  actor.continue(schedule_reconnect(
    State(..state, connector: None),
    ConnectorCrashed(exit.reason),
  ))
}

// -------------------------------------------------------------- connecting

/// Start a linked process that makes one connection attempt.
fn start_connecting(state: State) -> State {
  case state.status {
    Open(connection) -> set_connection(state, connection)
    Connecting -> state

    WaitingToRetry -> {
      let config = state.config
      let client = state.self
      let owner = process.self()

      let connector =
        process.spawn(fn() {
          process.send(client, ConnectFinished(open_connection(config, owner)))
        })

      State(..state, status: Connecting, connector: Some(connector))
    }
  }
}

/// Runs in the connector process. It connects, does the handshake and
/// hands over the socket.
fn open_connection(
  config: Builder,
  owner: Pid,
) -> Result(Established, Disconnect) {
  let deadline = clock.now_ms() + config.connect_timeout_ms

  use connecting <- result.try(
    transport.connect(
      host: config.host,
      port: config.port,
      security: config.security,
      limits: config.limits,
      timeout_ms: config.connect_timeout_ms,
    )
    |> result.map_error(fn(error) { Unreachable(error.ipv4, error.ipv6) }),
  )

  let established = {
    use Nil <- result.try(
      transport.send(connecting, connect_frame(config))
      |> result.map_error(to_disconnect),
    )

    use established <- result.try(handshake(config, connecting, deadline))

    transport.hand_over(established.transport, owner)
    |> result.map_error(to_disconnect)
    |> result.replace(established)
  }

  use reason <- result.map_error(established)

  transport.close(connecting)
  reason
}

fn connect_frame(config: Builder) -> Frame {
  let login = case config.login {
    Some(#(login, passcode)) -> [#("login", login), #("passcode", passcode)]
    None -> []
  }

  let heart_beat =
    int.to_string(config.heart_beat_send_ms)
    <> ","
    <> int.to_string(config.heart_beat_receive_ms)

  Frame(
    frame.Connect,
    list.flatten([
      [#("accept-version", "1.1,1.2"), #("host", config.virtual_host)],
      login,
      [#("heart-beat", heart_beat)],
      config.connect_headers,
    ]),
    <<>>,
  )
}

/// Wait for the broker's reply to CONNECT.
fn handshake(
  config: Builder,
  connecting: Transport,
  deadline: Int,
) -> Result(Established, Disconnect) {
  let timed_out = HandshakeTimedOut(config.connect_timeout_ms)

  use #(reply, connecting) <- result.try(
    transport.receive_frame(connecting, deadline)
    |> result.map_error(fn(ended) {
      ended_reason(ended) |> result.unwrap(timed_out)
    }),
  )

  case kind(reply.command) {
    IsError -> Error(Refused(error_message(reply), reply))
    IsMessage | IsReceipt | IsFromClient -> Error(UnexpectedFrame(reply))

    IsConnected -> {
      use session <- result.map(session(config, reply))

      Established(
        transport: transport.set_version(connecting, session.version),
        session:,
      )
    }
  }
}

/// Convert a failed read to a `Disconnect`. Timeouts return `Error`.
fn ended_reason(ended: transport.Ended) -> Result(Disconnect, Nil) {
  case ended {
    transport.TimedOut -> Error(Nil)
    transport.Closed -> Ok(Closed)
    transport.Failed(error:) -> Ok(SocketFailed(error))
    transport.Malformed(error:) -> Ok(Malformed(error))
  }
}

fn session(config: Builder, connected: Frame) -> Result(Session, Disconnect) {
  use version <- result.map(case frame.header(connected, "version") {
    Ok("1.2") -> Ok(frame.Stomp12)
    Ok("1.1") -> Ok(frame.Stomp11)
    Ok(other) -> Error(UnsupportedVersion(other))
    // No `version` header means the broker chose 1.0.
    Error(Nil) -> Error(UnsupportedVersion("1.0"))
  })

  let #(broker_sends, broker_wants) =
    frame.header(connected, "heart-beat")
    |> result.try(string.split_once(_, ","))
    |> result.try(fn(pair) {
      use sends <- result.try(int.parse(string.trim(pair.0)))
      use wants <- result.map(int.parse(string.trim(pair.1)))

      #(sends, wants)
    })
    |> result.unwrap(#(0, 0))

  Session(
    version:,
    server: option.from_result(frame.header(connected, "server")),
    id: option.from_result(frame.header(connected, "session")),
    heart_beat_send_ms: negotiate(config.heart_beat_send_ms, broker_wants),
    heart_beat_receive_ms: negotiate(config.heart_beat_receive_ms, broker_sends),
    headers: connected.headers,
  )
}

/// Agree a heart-beat interval. If either side says 0, it is off.
fn negotiate(ours: Int, theirs: Int) -> Int {
  use <- bool.guard(ours <= 0 || theirs <= 0, 0)

  int.max(ours, theirs)
}

fn connected(state: State, established: Established) -> State {
  let now = clock.now_ms()
  let session = established.session
  let connection = initial_connection(established, now)
  let state = State(..state, connection_id: state.connection_id + 1)

  case send_subscribes(state, connection, state.subscriptions) {
    Error(#(state, connection, reason)) ->
      fail_connection(state, connection, reason)

    Ok(#(state, connection)) -> {
      schedule_heart_beat(state, session)
      schedule_heart_beat_check(state, session)
      tell(state, Connected(session))
      reply_to_connection_waiters(state.connection_waiters, now)

      // The buffer may already contain frames after CONNECTED.
      handle_frames(State(..state, connection_waiters: []), connection)
    }
  }
}

fn initial_connection(established: Established, now: Int) -> Connection {
  Connection(
    transport: established.transport,
    session: established.session,
    since: now,
    pending_receipts: dict.new(),
    pending_subscribes: dict.new(),
    unhandled: 0,
    packet_requested: False,
    last_received_at: now,
  )
}

/// Send SUBSCRIBE for each subscription. Stop at the first failure.
fn send_subscribes(
  state: State,
  connection: Connection,
  subscriptions: List(Subscription),
) -> Result(#(State, Connection), #(State, Connection, Disconnect)) {
  case subscriptions {
    [] -> Ok(#(state, connection))

    [wanted, ..rest] -> {
      use #(state, connection) <- result.try(send_subscribe(
        state,
        connection,
        wanted,
      ))

      send_subscribes(state, connection, rest)
    }
  }
}

/// Reply to `await_connected` callers still within their deadline.
fn reply_to_connection_waiters(
  connection_waiters: List(#(Subject(Nil), Int)),
  now: Int,
) -> Nil {
  use #(reply, until) <- list.each(connection_waiters)
  use <- bool.guard(until <= now, Nil)

  process.send(reply, Nil)
}

/// Send SUBSCRIBE with a receipt header.
fn send_subscribe(
  state: State,
  connection: Connection,
  wanted: Subscription,
) -> Result(#(State, Connection), #(State, Connection, Disconnect)) {
  let #(state, receipt) = next_id(state)

  let ack = case wanted.ack {
    None -> "auto"
    Some(Cumulative) -> "client"
    Some(Individual) -> "client-individual"
  }

  let headers = [
    #("id", wanted.id),
    #("destination", wanted.destination),
    #("ack", ack),
    #("receipt", receipt),
    ..wanted.headers
  ]

  case write(connection, Frame(frame.Subscribe, headers, <<>>)) {
    Error(reason) -> Error(#(state, connection, reason))

    Ok(Nil) -> {
      let pending_subscribes =
        dict.insert(connection.pending_subscribes, receipt, wanted.id)

      Ok(#(state, Connection(..connection, pending_subscribes:)))
    }
  }
}

fn next_id(state: State) -> #(State, String) {
  let serial = state.serial + 1

  #(State(..state, serial:), "glomp-" <> int.to_string(serial))
}

// ------------------------------------------------------------ disconnecting

/// Close the connection and schedule a reconnect.
fn fail_connection(
  state: State,
  connection: Connection,
  reason: Disconnect,
) -> State {
  transport.close(connection.transport)
  fail_pending_receipts(connection.pending_receipts, reason)

  let lasted = clock.now_ms() - connection.since

  let failures = case lasted >= state.config.retry_longest_ms {
    True -> 0
    False -> state.failures
  }

  // Change `connection_id` so old deliveries and transactions expire.
  schedule_reconnect(
    State(..state, failures:, connection_id: state.connection_id + 1),
    reason,
  )
}

/// Reply `ConnectionLost` to callers waiting for receipts.
fn fail_pending_receipts(
  waiting: Dict(String, Subject(Result(Nil, Error))),
  reason: Disconnect,
) -> Nil {
  use reply <- list.each(dict.values(waiting))

  process.send(reply, Error(ConnectionLost(reason)))
}

fn schedule_reconnect(state: State, reason: Disconnect) -> State {
  let failures = state.failures + 1

  let delay =
    retry_delay(
      state.config.retry_first_ms,
      state.config.retry_longest_ms,
      failures,
    )

  process.send_after(state.self, delay, StartConnecting)
  tell(state, Disconnected(reason, delay))
  State(..state, status: WaitingToRetry, failures:)
}

fn retry_delay(delay: Int, longest: Int, failures: Int) -> Int {
  use <- bool.guard(failures <= 1 || delay >= longest, int.min(delay, longest))

  retry_delay(delay * 2, longest, failures - 1)
}

/// Send DISCONNECT if connected, then stop the helper processes.
fn shut_down(state: State) -> Stopped {
  // Stop the connector before it logs in.
  case state.connector {
    Some(connector) -> process.send_abnormal_exit(connector, otp.Shutdown)
    None -> Nil
  }

  let stopped = case state.status {
    WaitingToRetry | Connecting -> Idle
    Open(connection) -> send_disconnect(connection)
  }

  // An abnormal exit is needed to stop the handler process.
  process.send_abnormal_exit(state.handler_process, otp.Shutdown)
  stopped
}

/// Send DISCONNECT and wait for its receipt.
fn send_disconnect(connection: Connection) -> Stopped {
  let receipt = "glomp-disconnect"
  let disconnect = Frame(frame.Disconnect, [#("receipt", receipt)], <<>>)

  let sent = {
    use passive <- result.try(transport.set_passive(connection.transport))

    transport.send(passive, disconnect) |> result.replace(passive)
  }

  let #(stopped, waiting) = case sent {
    Error(failure) -> #(
      Abrupt(to_disconnect(failure)),
      connection.pending_receipts,
    )

    Ok(passive) -> {
      let deadline = clock.now_ms() + disconnect_timeout_ms

      await_disconnect_receipt(
        passive,
        receipt,
        connection.pending_receipts,
        deadline,
      )
    }
  }

  transport.close(connection.transport)
  fail_pending_receipts(waiting, Closed)

  stopped
}

/// Wait for the DISCONNECT receipt. Returns callers still waiting.
fn await_disconnect_receipt(
  from: Transport,
  receipt: String,
  waiting: Dict(String, Subject(Result(Nil, Error))),
  deadline: Int,
) -> #(Stopped, Dict(String, Subject(Result(Nil, Error)))) {
  case transport.receive_frame(from, deadline) {
    Error(ended) ->
      case ended_reason(ended) {
        Ok(reason) -> #(Abrupt(reason), waiting)
        Error(Nil) -> #(Unconfirmed, waiting)
      }

    Ok(#(received, from)) ->
      case kind(received.command) {
        IsError -> {
          let reason = BrokerError(error_message(received), received)

          #(Abrupt(reason), waiting)
        }

        // Ignore messages: the handler process is stopping.
        IsMessage | IsConnected | IsFromClient ->
          await_disconnect_receipt(from, receipt, waiting, deadline)

        IsReceipt -> {
          let id = frame.header(received, "receipt-id")
          use <- bool.guard(id == Ok(receipt), #(Graceful, waiting))

          // A receipt for another caller. Reply to them.
          let waiting =
            result.try(id, resolve_receipt(waiting, _))
            |> result.unwrap(waiting)

          await_disconnect_receipt(from, receipt, waiting, deadline)
        }
      }
  }
}

// ---------------------------------------------------------------- reading

fn socket_message(state: State, incoming: socket.Incoming) -> State {
  use connection <- with_connection(state)

  case transport.arrival(connection.transport, incoming) {
    transport.Stale -> state

    transport.Received(transport:) ->
      handle_frames(state, with_received(connection, transport))

    transport.PeerClosed -> fail_connection(state, connection, Closed)

    transport.Errored(error:) ->
      fail_connection(state, connection, SocketFailed(error))
  }
}

/// Handle all buffered frames, then request the next packet.
fn handle_frames(state: State, connection: Connection) -> State {
  case transport.next(connection.transport) {
    Error(error) -> fail_connection(state, connection, Malformed(error))

    Ok(transport.Empty(transport:)) ->
      request_packet(state, Connection(..connection, transport:))

    Ok(transport.Frame(frame: received, transport:)) -> {
      let connection = Connection(..connection, transport:)

      case kind(received.command) {
        IsMessage -> handle_frames(state, deliver(state, connection, received))
        IsReceipt -> handle_frames(state, receipt(state, connection, received))

        IsError -> {
          let reason = BrokerError(error_message(received), received)

          fail_connection(state, connection, reason)
        }

        IsConnected | IsFromClient ->
          fail_connection(state, connection, UnexpectedFrame(received))
      }
    }
  }
}

/// Request the next packet while fewer than 64 messages are unhandled.
///
/// Pausing at 64 stops slow handlers from filling memory.
fn request_packet(state: State, connection: Connection) -> State {
  let paused =
    connection.packet_requested || connection.unhandled >= max_unhandled

  use <- bool.guard(paused, set_connection(state, connection))

  case transport.set_active_once(connection.transport) {
    Error(failure) -> fail_connection(state, connection, to_disconnect(failure))

    // Reading was paused, so restart the idle timer.
    Ok(Nil) -> {
      let last_received_at = clock.now_ms()

      set_connection(
        state,
        Connection(..connection, packet_requested: True, last_received_at:),
      )
    }
  }
}

/// Send a MESSAGE to the handler process.
fn deliver(
  state: State,
  connection: Connection,
  received: Frame,
) -> Connection {
  let required = fn(name) {
    frame.header(received, name) |> result.replace_error(MissingHeader(name))
  }

  let parsed = {
    use id <- result.try(required("subscription"))
    use message_id <- result.try(required("message-id"))
    use destination <- result.try(required("destination"))

    use holder <- result.try(
      list.find(state.subscriptions, fn(held) { held.id == id })
      |> result.replace_error(UnknownSubscription(id)),
    )

    use headers <- result.map(case holder.ack, connection.session.version {
      None, frame.Stomp11 | None, frame.Stomp12 -> Ok([])

      Some(Cumulative), frame.Stomp11 | Some(Individual), frame.Stomp11 ->
        Ok([#("message-id", message_id), #("subscription", id)])

      Some(Cumulative), frame.Stomp12 | Some(Individual), frame.Stomp12 ->
        required("ack") |> result.map(fn(ack) { [#("id", ack)] })
    })

    Deliver(
      Message(
        destination:,
        id: message_id,
        subscription: id,
        headers: received.headers,
        body: received.body,
      ),
      Delivery(connection_id: state.connection_id, headers:),
    )
  }

  case parsed {
    Ok(delivery) -> {
      process.send(state.handlers, delivery)
      Connection(..connection, unhandled: connection.unhandled + 1)
    }

    Error(reason) -> {
      tell(state, Dropped(received, reason))
      connection
    }
  }
}

/// Handle a RECEIPT for a caller or a SUBSCRIBE.
fn receipt(
  state: State,
  connection: Connection,
  received: Frame,
) -> Connection {
  let dropped = fn(reason) {
    tell(state, Dropped(received, reason))
    connection
  }

  case frame.header(received, "receipt-id") {
    Error(Nil) -> dropped(MissingHeader("receipt-id"))

    Ok(id) ->
      resolve_caller_receipt(connection, id)
      |> result.lazy_or(fn() {
        resolve_subscribe_receipt(state, connection, id)
      })
      |> result.lazy_unwrap(fn() { dropped(UnknownReceipt(id)) })
  }
}

fn resolve_caller_receipt(
  connection: Connection,
  id: String,
) -> Result(Connection, Nil) {
  use pending_receipts <- result.map(resolve_receipt(
    connection.pending_receipts,
    id,
  ))

  Connection(..connection, pending_receipts:)
}

fn resolve_subscribe_receipt(
  state: State,
  connection: Connection,
  id: String,
) -> Result(Connection, Nil) {
  use subscription <- result.map(dict.get(connection.pending_subscribes, id))

  tell(state, Subscribed(subscription))

  Connection(
    ..connection,
    pending_subscribes: dict.delete(connection.pending_subscribes, id),
  )
}

/// A handler returned. Send its ACK or NACK.
fn handler_finished(
  state: State,
  connection_id: Int,
  acknowledge: Option(#(Delivery, Acknowledgement)),
) -> State {
  // Ignore results from an earlier connection.
  use connection <- with_current_connection(state, connection_id)

  let connection = Connection(..connection, unhandled: connection.unhandled - 1)

  case acknowledge {
    None -> request_packet(state, connection)

    Some(#(delivery, settlement)) -> {
      let frame = acknowledgement_frame(delivery, settlement, None)
      use <- try_write(state, connection, frame)

      request_packet(state, connection)
    }
  }
}

// ------------------------------------------------------------- heart-beats

fn schedule_heart_beat(state: State, session: Session) -> Nil {
  use <- bool.guard(session.heart_beat_send_ms <= 0, Nil)

  process.send_after(
    state.self,
    int.max(session.heart_beat_send_ms / 2, 1),
    HeartBeatDue(state.connection_id),
  )

  Nil
}

fn schedule_heart_beat_check(state: State, session: Session) -> Nil {
  use <- bool.guard(session.heart_beat_receive_ms <= 0, Nil)

  process.send_after(
    state.self,
    session.heart_beat_receive_ms,
    HeartBeatCheckDue(state.connection_id),
  )

  Nil
}

fn send_heart_beat(state: State, connection_id: Int) -> State {
  use connection <- with_current_connection(state, connection_id)

  schedule_heart_beat(state, connection.session)

  case transport.send_heart_beat(connection.transport) {
    Ok(Nil) -> state
    Error(failure) -> fail_connection(state, connection, to_disconnect(failure))
  }
}

fn check_heart_beat(state: State, connection_id: Int) -> State {
  use connection <- with_current_connection(state, connection_id)

  schedule_heart_beat_check(state, connection.session)

  let idle_ms = clock.now_ms() - connection.last_received_at
  let allowed = connection.session.heart_beat_receive_ms * 2

  // Check only while a packet is requested.
  use <- bool.guard(!connection.packet_requested || idle_ms < allowed, state)

  // A packet may already be waiting in the mailbox.
  case transport.take_delivered(connection.transport) {
    Error(Nil) -> fail_connection(state, connection, HeartBeatMissed(idle_ms))
    Ok(transport) -> handle_frames(state, with_received(connection, transport))
  }
}

// ---------------------------------------------------------------- writing

fn write(connection: Connection, frame: Frame) -> Result(Nil, Disconnect) {
  transport.send(connection.transport, frame) |> result.map_error(to_disconnect)
}

fn to_disconnect(failure: socket.Failure) -> Disconnect {
  case failure {
    socket.Closed -> Closed
    socket.TimedOut -> SocketFailed(network.TimedOut)
    socket.Failed(error:) -> SocketFailed(error)
  }
}

/// Get the connection. Fails if the transaction has expired.
fn current(
  state: State,
  transaction: Option(Transaction),
) -> Result(Connection, Error) {
  let stale = case transaction {
    Some(transaction) -> transaction.connection_id != state.connection_id
    None -> False
  }

  use <- bool.guard(stale, Error(Expired))

  case state.status {
    Open(connection) -> Ok(connection)
    WaitingToRetry | Connecting -> Error(NotConnected)
  }
}

/// Run `then` with the connection, or reply with the error.
fn connection_for(
  state: State,
  transaction: Option(Transaction),
  reply: Subject(Result(answer, Error)),
  then: fn(Connection) -> State,
) -> State {
  case current(state, transaction) {
    Ok(connection) -> then(connection)
    Error(error) -> reply_error(state, reply, error)
  }
}

/// Write a frame. Reply now, or when its receipt arrives.
fn write_for_caller(
  state: State,
  transaction: Option(Transaction),
  frame: Frame,
  confirm_within_ms: Option(Int),
  reply: Subject(Result(Nil, Error)),
) -> State {
  use connection <- connection_for(state, transaction, reply)

  case confirm_within_ms {
    Some(within_ms) ->
      write_with_receipt(state, connection, frame, within_ms, reply)

    None -> {
      use <- try_write_for(state, connection, frame, reply)

      process.send(reply, Ok(Nil))
      state
    }
  }
}

/// Write a frame with a receipt header. Reply on receipt.
fn write_with_receipt(
  state: State,
  connection: Connection,
  frame: Frame,
  within_ms: Int,
  reply: Subject(Result(Nil, Error)),
) -> State {
  let #(state, receipt) = next_id(state)
  let headers = list.append(frame.headers, [#("receipt", receipt)])
  use <- try_write_for(state, connection, Frame(..frame, headers:), reply)

  process.send_after(state.self, within_ms, ReceiptOverdue(receipt, within_ms))

  let pending_receipts =
    dict.insert(connection.pending_receipts, receipt, reply)

  set_connection(state, Connection(..connection, pending_receipts:))
}

fn receipt_overdue(state: State, receipt: String, waited_ms: Int) -> State {
  use connection <- with_connection(state)

  case dict.get(connection.pending_receipts, receipt) {
    // The caller was already replied to.
    Error(Nil) -> state

    Ok(reply) -> {
      process.send(reply, Error(NoReceipt(waited_ms)))

      let pending_receipts = dict.delete(connection.pending_receipts, receipt)

      set_connection(state, Connection(..connection, pending_receipts:))
    }
  }
}

fn outbound_frame(outbound: Outbound) -> Frame {
  let content_type = case outbound.content_type {
    Some(content_type) -> [#("content-type", content_type)]
    None -> []
  }

  let content_length = case
    outbound.content_length || bytes.has_nul(outbound.body)
  {
    True -> [
      #("content-length", int.to_string(bit_array.byte_size(outbound.body))),
    ]

    False -> []
  }

  let transaction = case outbound.transaction {
    Some(transaction) -> [#("transaction", transaction.id)]
    None -> []
  }

  Frame(
    frame.Send,
    list.flatten([
      [#("destination", outbound.destination)],
      content_type,
      content_length,
      transaction,
      outbound.headers,
    ]),
    outbound.body,
  )
}

fn acknowledgement_frame(
  delivery: Delivery,
  settlement: Acknowledgement,
  transaction: Option(Transaction),
) -> Frame {
  let command = case settlement {
    SendAck -> frame.Ack
    SendNack -> frame.Nack
  }

  let transaction = case transaction {
    Some(transaction) -> [#("transaction", transaction.id)]
    None -> []
  }

  Frame(command, list.append(delivery.headers, transaction), <<>>)
}

fn acknowledging(
  state: State,
  delivery: Delivery,
  settlement: Acknowledgement,
  transaction: Option(Transaction),
  reply: Subject(Result(Nil, Error)),
) -> State {
  let stale = delivery.connection_id != state.connection_id
  use <- bool.lazy_guard(stale, fn() { reply_error(state, reply, Expired) })

  let frame = acknowledgement_frame(delivery, settlement, transaction)

  write_for_caller(state, transaction, frame, None, reply)
}

fn begin_now(
  state: State,
  reply: Subject(Result(Transaction, Error)),
) -> State {
  use connection <- connection_for(state, None, reply)

  let #(state, id) = next_id(state)
  let begin = Frame(frame.Begin, [#("transaction", id)], <<>>)
  use <- try_write_for(state, connection, begin, reply)

  process.send(reply, Ok(Transaction(connection_id: state.connection_id, id:)))
  state
}

fn subscribe_now(
  state: State,
  wanted: Subscription,
  reply: Subject(Result(Nil, AlreadySubscribed)),
) -> State {
  let held = list.any(state.subscriptions, fn(held) { held.id == wanted.id })

  use <- bool.lazy_guard(held, fn() {
    reply_error(state, reply, AlreadySubscribed(wanted.id))
  })

  process.send(state.handlers, Register(wanted.id, wanted.handler))
  process.send(reply, Ok(Nil))

  let subscriptions = list.append(state.subscriptions, [wanted])
  let state = State(..state, subscriptions:)
  use connection <- with_connection(state)

  case send_subscribe(state, connection, wanted) {
    Ok(#(state, connection)) -> set_connection(state, connection)

    Error(#(state, connection, reason)) ->
      fail_connection(state, connection, reason)
  }
}

fn unsubscribe_now(
  state: State,
  id: String,
  reply: Subject(Result(Nil, NotSubscribed)),
) -> State {
  let held = list.any(state.subscriptions, fn(held) { held.id == id })

  use <- bool.lazy_guard(!held, fn() {
    reply_error(state, reply, NotSubscribed(id))
  })

  process.send(state.handlers, Forget(id))
  process.send(reply, Ok(Nil))

  let subscriptions =
    list.filter(state.subscriptions, fn(held) { held.id != id })

  let state = State(..state, subscriptions:)
  use connection <- with_connection(state)

  let unsubscribe = Frame(frame.Unsubscribe, [#("id", id)], <<>>)
  use <- try_write(state, connection, unsubscribe)

  // Stop waiting for this subscription's SUBSCRIBE receipt.
  let pending_subscribes =
    dict.to_list(connection.pending_subscribes)
    |> list.filter(fn(pending) { pending.1 != id })
    |> dict.from_list

  set_connection(state, Connection(..connection, pending_subscribes:))
}

// ----------------------------------------------------------------- frames

type Kind {
  IsConnected
  IsMessage
  IsReceipt
  IsError
  /// A command that only clients send.
  IsFromClient
}

fn kind(command: frame.Command) -> Kind {
  case command {
    frame.Connected -> IsConnected
    frame.Message -> IsMessage
    frame.Receipt -> IsReceipt
    frame.ServerError -> IsError

    frame.Connect
    | frame.Stomp
    | frame.Send
    | frame.Subscribe
    | frame.Unsubscribe
    | frame.Ack
    | frame.Nack
    | frame.Begin
    | frame.Commit
    | frame.Abort
    | frame.Disconnect -> IsFromClient
  }
}

fn error_message(error: Frame) -> String {
  frame.header(error, "message") |> result.unwrap("")
}
