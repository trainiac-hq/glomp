//// The client's TCP and TLS sockets. This module connects to the broker,
//// tries IPv4 then IPv6, and moves bytes in and out. It deals only in
//// bytes, and `transport` turns them into frames.

import gleam/bool
import gleam/dynamic.{type Dynamic}
import gleam/erlang/atom
import gleam/erlang/charlist.{type Charlist}
import gleam/erlang/process.{type Pid, type Selector}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import glomp/internal/clock
import glomp/network.{type NetworkError, type Trust}

pub opaque type Socket {
  Tcp(handle: TcpSocket)
  Ssl(handle: SslSocket)
}

/// A `gen_tcp` socket.
type TcpSocket

/// An `ssl` socket.
type SslSocket

/// An error term from `gen_tcp`, `inet` or `ssl`.
type Reason

/// Certificate authority certificates, DER encoded.
type Certificates

/// A function that checks a certificate's host name.
type HostnameMatch

pub type Security {
  Plain
  Tls(trust: Trust)
}

/// Connecting over IPv4 failed, and so did IPv6. IPv6 is only tried if
/// there is time left.
pub type ConnectError {
  ConnectError(ipv4: NetworkError, ipv6: Option(NetworkError))
}

pub type Failure {
  Closed
  TimedOut
  Failed(error: NetworkError)
}

/// A message from a socket in active-once mode.
pub type Incoming {
  Incoming(socket: Socket, event: Event)
}

pub type Event {
  Arrived(bytes: BitArray)
  Ended
  Errored(error: NetworkError)
}

/// Options for `gen_tcp:connect` and `ssl:connect`.
type SocketOption {
  Inet
  Inet6
  Binary
  Active(Bool)
  Packet(Packet)
  Nodelay(Bool)
  Keepalive(Bool)
  SendTimeout(milliseconds: Int)
  SendTimeoutClose(Bool)
  Verify(Verify)
  Depth(Int)
  Cacerts(Certificates)
  Cacertfile(path: Charlist)
  CustomizeHostnameCheck(List(HostnameCheck))
}

type Packet {
  Raw
}

type Verify {
  VerifyPeer
  VerifyNone
}

type HostnameCheck {
  MatchFun(HostnameMatch)
}

type Protocol {
  Https
}

// ------------------------------------------------------------- connecting

/// Connect over IPv4, then over IPv6. Both attempts share one timeout.
pub fn connect(
  host: String,
  port: Int,
  security: Security,
  timeout_ms: Int,
) -> Result(Socket, ConnectError) {
  let started = clock.now_ms()
  let host = charlist.from_string(host)

  use connector <- result.try(
    connector(security) |> result.map_error(ConnectError(_, None)),
  )

  let open = fn(family, timeout) {
    connector.connect(host, port, [family, ..connector.options], timeout)
  }

  use ipv4 <- result.try_recover(open(Inet, timeout_ms))

  let remaining = timeout_ms - { clock.now_ms() - started }
  use <- bool.guard(remaining <= 0, Error(ConnectError(ipv4, None)))

  use ipv6 <- result.map_error(open(Inet6, remaining))

  ConnectError(ipv4, Some(ipv6))
}

/// How to connect: the function to call and its options.
type Connector {
  Connector(
    connect: fn(Charlist, Int, List(SocketOption), Int) ->
      Result(Socket, NetworkError),
    options: List(SocketOption),
  )
}

const tcp = [
  Binary,
  Active(False),
  Packet(Raw),
  Nodelay(True),
  Keepalive(True),
  SendTimeout(15_000),
  SendTimeoutClose(True),
]

fn connector(security: Security) -> Result(Connector, NetworkError) {
  case security {
    Plain -> Ok(Connector(connect: connect_tcp, options: tcp))

    Tls(trust:) -> {
      use Nil <- result.try(start_ssl())
      use tls <- result.map(trust_options(trust))
      Connector(connect: connect_tls, options: list.append(tls, tcp))
    }
  }
}

fn connect_tcp(
  host: Charlist,
  port: Int,
  options: List(SocketOption),
  timeout_ms: Int,
) -> Result(Socket, NetworkError) {
  tcp_connect(host, port, options, timeout_ms)
  |> result.map(Tcp)
  |> result.map_error(network_error)
}

fn connect_tls(
  host: Charlist,
  port: Int,
  options: List(SocketOption),
  timeout_ms: Int,
) -> Result(Socket, NetworkError) {
  ssl_connect(host, port, options, timeout_ms)
  |> result.map(Ssl)
  |> result.map_error(network_error)
}

fn trust_options(trust: Trust) -> Result(List(SocketOption), NetworkError) {
  case trust {
    network.Unverified -> Ok([Verify(VerifyNone)])

    network.CertificateFile(path:) ->
      Ok([Cacertfile(charlist.from_string(path)), ..verify_peer()])

    network.SystemCertificates -> {
      use certificates <- result.map(system_certificates())

      [Cacerts(certificates), ..verify_peer()]
    }
  }
}

fn verify_peer() -> List(SocketOption) {
  [
    Verify(VerifyPeer),
    Depth(8),
    CustomizeHostnameCheck([MatchFun(hostname_match_fun(Https))]),
  ]
}

// ---------------------------------------------------------------- using

@external(erlang, "glomp_socket_ffi", "send")
pub fn send(socket: Socket, data: BitArray) -> Result(Nil, Failure)

/// Block until bytes arrive or `timeout_ms` passes.
@external(erlang, "glomp_socket_ffi", "recv")
pub fn receive(socket: Socket, timeout_ms: Int) -> Result(BitArray, Failure)

/// Deliver the next packet to the owner as a message.
@external(erlang, "glomp_socket_ffi", "set_active_once")
pub fn set_active_once(socket: Socket) -> Result(Nil, Failure)

/// Stop delivering packets as messages.
@external(erlang, "glomp_socket_ffi", "set_passive")
pub fn set_passive(socket: Socket) -> Result(Nil, Failure)

/// Make `to` the process that owns the socket.
@external(erlang, "glomp_socket_ffi", "hand_over")
pub fn hand_over(socket: Socket, to: Pid) -> Result(Nil, Failure)

@external(erlang, "glomp_socket_ffi", "close")
pub fn close(socket: Socket) -> Nil

/// Take bytes the socket already delivered to this mailbox.
@external(erlang, "glomp_socket_ffi", "take_delivered")
pub fn take_delivered(socket: Socket) -> BitArray

/// Select messages from sockets in active-once mode.
pub fn select(
  selector: Selector(message),
  mapping: fn(Incoming) -> message,
) -> Selector(message) {
  let map = fn(message) { mapping(incoming(message)) }

  selector
  |> process.select_record(atom.create("tcp"), 2, map)
  |> process.select_record(atom.create("tcp_closed"), 1, map)
  |> process.select_record(atom.create("tcp_error"), 2, map)
  |> process.select_record(atom.create("ssl"), 2, map)
  |> process.select_record(atom.create("ssl_closed"), 1, map)
  |> process.select_record(atom.create("ssl_error"), 2, map)
}

// -------------------------------------------------------------- externals

@external(erlang, "gen_tcp", "connect")
fn tcp_connect(
  host: Charlist,
  port: Int,
  options: List(SocketOption),
  timeout_ms: Int,
) -> Result(TcpSocket, Reason)

@external(erlang, "ssl", "connect")
fn ssl_connect(
  host: Charlist,
  port: Int,
  options: List(SocketOption),
  timeout_ms: Int,
) -> Result(SslSocket, Reason)

@external(erlang, "glomp_socket_ffi", "start_ssl")
fn start_ssl() -> Result(Nil, NetworkError)

@external(erlang, "public_key", "pkix_verify_hostname_match_fun")
fn hostname_match_fun(protocol: Protocol) -> HostnameMatch

@external(erlang, "glomp_socket_ffi", "system_certificates")
fn system_certificates() -> Result(Certificates, NetworkError)

@external(erlang, "glomp_socket_ffi", "incoming")
fn incoming(message: Dynamic) -> Incoming

@external(erlang, "glomp_socket_ffi", "network_error")
fn network_error(reason: Reason) -> NetworkError
