//// Encode and decode STOMP frames yourself, without any sockets. The client
//// in `glomp` uses this module, so most users never need it. You can use this
//// if you are writing your own STOMP transport, or a broker!

import gleam/bit_array
import gleam/bool
import gleam/int
import gleam/list
import gleam/pair
import gleam/result
import gleam/string
import glomp/internal/bytes

/// A STOMP command.
pub type Command {
  Connect
  /// Another name for CONNECT, added in STOMP 1.2.
  Stomp
  Send
  Subscribe
  Unsubscribe
  Ack
  Nack
  Begin
  Commit
  Abort
  Disconnect
  Connected
  Message
  Receipt
  /// The ERROR command, which a broker sends when something went wrong.
  ServerError
}

/// A frame, which is one unit of STOMP: a command, some headers and a body.
///
/// Its headers are unescaped and kept in the order they were sent.
pub type Frame {
  Frame(command: Command, headers: List(#(String, String)), body: BitArray)
}

pub type Version {
  Stomp11
  Stomp12
}

pub type Limits {
  Limits(max_head_bytes: Int, max_body_bytes: Int)
}

/// The default limits, which allow 64 KiB of headers and a 100 MiB body.
pub const default_limits =
  Limits(max_head_bytes: 65_536, max_body_bytes: 104_857_600)

/// An error from turning bytes into a frame. After any of these errors the
/// decoder cannot find where the next frame starts, so the stream has to
/// be closed.
pub type DecodeError {
  /// The first line is not a known STOMP command.
  UnknownCommand(command: String)
  /// The command or headers are not UTF-8.
  HeadNotUtf8
  /// A header line is missing its colon or its name.
  MalformedHeader(line: String)
  /// A header uses an escape that STOMP does not define. The valid escapes
  /// are `\n`, `\c` and `\\`, plus `\r` in 1.2.
  UndefinedEscape(sequence: String)
  /// The `content-length` header is not a valid number of bytes.
  InvalidContentLength(value: String)
  /// The body does not end with a NUL byte.
  MissingTerminator
  HeadTooLarge(limit: Int)
  BodyTooLarge(limit: Int)
}

/// Get the command's name on the transport, such as `"SEND"`.
pub fn command_to_string(command: Command) -> String {
  case command {
    Connect -> "CONNECT"
    Stomp -> "STOMP"
    Send -> "SEND"
    Subscribe -> "SUBSCRIBE"
    Unsubscribe -> "UNSUBSCRIBE"
    Ack -> "ACK"
    Nack -> "NACK"
    Begin -> "BEGIN"
    Commit -> "COMMIT"
    Abort -> "ABORT"
    Disconnect -> "DISCONNECT"
    Connected -> "CONNECTED"
    Message -> "MESSAGE"
    Receipt -> "RECEIPT"
    ServerError -> "ERROR"
  }
}

fn command_from_string(command: String) -> Result(Command, DecodeError) {
  case command {
    "CONNECT" -> Ok(Connect)
    "STOMP" -> Ok(Stomp)
    "SEND" -> Ok(Send)
    "SUBSCRIBE" -> Ok(Subscribe)
    "UNSUBSCRIBE" -> Ok(Unsubscribe)
    "ACK" -> Ok(Ack)
    "NACK" -> Ok(Nack)
    "BEGIN" -> Ok(Begin)
    "COMMIT" -> Ok(Commit)
    "ABORT" -> Ok(Abort)
    "DISCONNECT" -> Ok(Disconnect)
    "CONNECTED" -> Ok(Connected)
    "MESSAGE" -> Ok(Message)
    "RECEIPT" -> Ok(Receipt)
    "ERROR" -> Ok(ServerError)
    unknown -> Error(UnknownCommand(unknown))
  }
}

/// Get the first header called `name`.
pub fn header(frame: Frame, name: String) -> Result(String, Nil) {
  list.key_find(frame.headers, name)
}

/// Encode a frame.
///
/// Headers are escaped for `version`. CONNECT and CONNECTED headers are
/// written as they are, because STOMP 1.0 brokers expect that.
///
/// Only the headers you give are written. A NUL byte marks the end of a
/// frame, so a body that contains one needs `content-length`. The reader
/// then knows how far to read.
pub fn encode(frame: Frame, version: Version) -> BitArray {
  let head =
    list.fold(
      frame.headers,
      command_to_string(frame.command) <> "\n",
      fn(head, header) {
        head
        <> escape(header.0, frame.command, version)
        <> ":"
        <> escape(header.1, frame.command, version)
        <> "\n"
      },
    )

  <<head:utf8, "\n":utf8, frame.body:bits, 0>>
}

/// A heart-beat, which is a single line feed.
pub const heart_beat = <<"\n":utf8>>

fn escaped(command: Command) -> Bool {
  case command {
    Connect | Stomp | Connected -> False

    Send
    | Subscribe
    | Unsubscribe
    | Ack
    | Nack
    | Begin
    | Commit
    | Abort
    | Disconnect
    | Message
    | Receipt
    | ServerError -> True
  }
}

fn escape(text: String, command: Command, version: Version) -> String {
  use <- bool.guard(!escaped(command), text)

  let text =
    text
    |> string.replace("\\", "\\\\")
    |> string.replace("\n", "\\n")
    |> string.replace(":", "\\c")

  case version {
    Stomp11 -> text
    Stomp12 -> string.replace(text, "\r", "\\r")
  }
}

fn unescape(
  text: String,
  version: Version,
  done: String,
) -> Result(String, DecodeError) {
  case string.split_once(text, "\\") {
    Error(Nil) -> Ok(done <> text)

    Ok(#(before, after)) ->
      case after, version {
        "\\" <> rest, Stomp11 | "\\" <> rest, Stomp12 ->
          unescape(rest, version, done <> before <> "\\")

        "n" <> rest, Stomp11 | "n" <> rest, Stomp12 ->
          unescape(rest, version, done <> before <> "\n")

        "c" <> rest, Stomp11 | "c" <> rest, Stomp12 ->
          unescape(rest, version, done <> before <> ":")

        // 1.1 defines no `\r` escape.
        "r" <> rest, Stomp12 -> unescape(rest, version, done <> before <> "\r")

        undefined, Stomp11 | undefined, Stomp12 ->
          Error(UndefinedEscape("\\" <> string.slice(undefined, 0, 1)))
      }
  }
}

/// A decoder turns bytes from the network into frames. It holds the bytes
/// that have arrived and how far it got through the next frame.
///
/// Create one with `decoder`. Pass it bytes with `feed`, then call `next`
/// until it returns `Incomplete`:
///
/// ```gleam
/// let decoder = frame.decoder(frame.default_limits) |> frame.feed(bytes)
///
/// case frame.next(decoder) {
///   Ok(frame.Decoded(frame:, decoder:)) -> use_frame(frame, decoder)
///   Ok(frame.Incomplete(decoder:)) -> wait_for_more_bytes(decoder)
///   Error(error) -> close_the_connection(error)
/// }
/// ```
///
/// Each step returns a new `Decoder`, so always keep the latest one. After
/// a `DecodeError`, the decoder cannot read any more of the stream.
pub opaque type Decoder {
  Decoder(
    limits: Limits,
    version: Version,
    buffer: BitArray,
    progress: Progress,
  )
}

type Progress {
  /// Searching for the end of the head, from byte `scanned`.
  InHead(scanned: Int)
  InBody(command: Command, headers: List(#(String, String)), end: BodyEnd)
}

type BodyEnd {
  /// The body length from `content-length`.
  Declared(length: Int)
  /// Searching for the body's NUL, from byte `scanned`.
  AtNul(scanned: Int)
}

/// What `next` gives back. It is either a frame, or a sign that more bytes
/// are needed.
///
/// Both variants carry the updated `Decoder`. Use it for the next call to
/// `feed` or `next`.
pub type Step {
  /// A frame. Call `next` again to get the one after it.
  Decoded(frame: Frame, decoder: Decoder)
  /// The buffer does not hold a whole frame yet. Call `feed` with more
  /// bytes.
  Incomplete(decoder: Decoder)
}

/// Create a decoder that reads STOMP 1.2. Change it with `set_version`.
pub fn decoder(limits: Limits) -> Decoder {
  Decoder(limits:, version: Stomp12, buffer: <<>>, progress: InHead(0))
}

/// Set the version to read by. Call it after the CONNECTED frame, because
/// that frame says which version the broker chose.
///
/// In 1.1, a carriage return is part of the data, and the `\r` escape is
/// invalid.
pub fn set_version(decoder: Decoder, version: Version) -> Decoder {
  Decoder(..decoder, version:)
}

/// Add bytes that arrived from the network.
pub fn feed(decoder: Decoder, bytes: BitArray) -> Decoder {
  Decoder(..decoder, buffer: <<decoder.buffer:bits, bytes:bits>>)
}

/// Take the next frame from the buffer. It skips heart-beats.
pub fn next(decoder: Decoder) -> Result(Step, DecodeError) {
  case decoder.progress {
    InHead(scanned:) -> head(decoder, scanned)

    InBody(command:, headers:, end:) ->
      body(decoder, decoder.buffer, command, headers, end)
  }
}

/// The byte sequences that end a head.
fn head_ends(version: Version) -> List(BitArray) {
  case version {
    Stomp11 -> [<<"\n\n":utf8>>]
    Stomp12 -> [<<"\n\n":utf8>>, <<"\n\r\n":utf8>>]
  }
}

fn head(decoder: Decoder, scanned: Int) -> Result(Step, DecodeError) {
  let limit = decoder.limits.max_head_bytes
  let before = bit_array.byte_size(decoder.buffer)
  let buffer = skip_line_ends(decoder.buffer, decoder.version)
  let size = bit_array.byte_size(buffer)
  // Start before `scanned`, since the ending may span chunks.
  let from = int.max(scanned - { before - size } - 2, 0)

  case bytes.find(buffer, head_ends(decoder.version), from) {
    Error(Nil) -> {
      use <- bool.guard(size > limit, Error(HeadTooLarge(limit)))

      Ok(Incomplete(Decoder(..decoder, buffer:, progress: InHead(size))))
    }

    Ok(#(position, length)) -> {
      use <- bool.guard(position > limit, Error(HeadTooLarge(limit)))

      let head = bytes.slice(buffer, 0, position)
      use #(command, headers) <- result.try(parse_head(head, decoder.version))
      use end <- result.try(body_end(headers, decoder.limits))

      body(
        decoder,
        bytes.drop(buffer, position + length),
        command,
        headers,
        end,
      )
    }
  }
}

fn body(
  decoder: Decoder,
  buffer: BitArray,
  command: Command,
  headers: List(#(String, String)),
  end: BodyEnd,
) -> Result(Step, DecodeError) {
  let limit = decoder.limits.max_body_bytes
  let size = bit_array.byte_size(buffer)

  let decoded = fn(length) {
    let rest = bytes.drop(buffer, length + 1)

    Ok(Decoded(
      Frame(command:, headers:, body: bytes.slice(buffer, 0, length)),
      Decoder(..decoder, buffer: rest, progress: InHead(0)),
    ))
  }

  case end {
    Declared(length:) -> {
      use <- bool.guard(
        size <= length,
        Ok(Incomplete(
          Decoder(
            ..decoder,
            buffer:,
            progress: InBody(command:, headers:, end:),
          ),
        )),
      )

      let terminated = bytes.slice(buffer, length, 1) == <<0>>
      use <- bool.guard(!terminated, Error(MissingTerminator))

      decoded(length)
    }

    AtNul(scanned:) -> {
      let nul = bytes.find(buffer, [<<0>>], scanned) |> result.map(pair.first)
      let so_far = result.unwrap(nul, size)
      use <- bool.guard(so_far > limit, Error(BodyTooLarge(limit)))

      case nul {
        Ok(position) -> decoded(position)
        Error(Nil) ->
          Ok(Incomplete(
            Decoder(
              ..decoder,
              buffer:,
              progress: InBody(command:, headers:, end: AtNul(size)),
            ),
          ))
      }
    }
  }
}

fn skip_line_ends(buffer: BitArray, version: Version) -> BitArray {
  case buffer, version {
    <<"\n":utf8, rest:bits>>, Stomp11 | <<"\n":utf8, rest:bits>>, Stomp12 ->
      skip_line_ends(rest, version)

    <<"\r\n":utf8, rest:bits>>, Stomp12 -> skip_line_ends(rest, version)
    frame, Stomp11 | frame, Stomp12 -> frame
  }
}

/// The command line, then one `name:value` header per line.
fn parse_head(
  head: BitArray,
  version: Version,
) -> Result(#(Command, List(#(String, String))), DecodeError) {
  use text <- result.try(
    bit_array.to_string(head) |> result.replace_error(HeadNotUtf8),
  )

  let lines = case version {
    Stomp11 -> string.split(text, "\n")
    Stomp12 -> string.split(text, "\n") |> list.map(without_carriage_return)
  }

  case lines {
    [] -> Error(UnknownCommand(""))

    [first, ..rest] -> {
      use command <- result.try(command_from_string(first))

      use headers <- result.map(
        list.try_map(rest, parse_header(_, escaped(command), version)),
      )

      #(command, headers)
    }
  }
}

fn without_carriage_return(line: String) -> String {
  case string.ends_with(line, "\r") {
    True -> string.drop_end(line, 1)
    False -> line
  }
}

fn parse_header(
  line: String,
  escaped: Bool,
  version: Version,
) -> Result(#(String, String), DecodeError) {
  // Split at the first colon. Later colons belong to the value.
  use #(name, value) <- result.try(
    string.split_once(line, ":") |> result.replace_error(MalformedHeader(line)),
  )

  // STOMP requires a header name to be at least one byte.
  use <- bool.guard(name == "", Error(MalformedHeader(line)))

  case escaped {
    False -> Ok(#(name, value))

    True -> {
      use name <- result.try(unescape(name, version, ""))
      use value <- result.map(unescape(value, version, ""))

      #(name, value)
    }
  }
}

fn body_end(
  headers: List(#(String, String)),
  limits: Limits,
) -> Result(BodyEnd, DecodeError) {
  case list.key_find(headers, "content-length") {
    Error(Nil) -> Ok(AtNul(0))

    Ok(value) -> {
      let invalid = Error(InvalidContentLength(value))

      use length <- result.try(
        int.parse(value) |> result.replace_error(InvalidContentLength(value)),
      )

      use <- bool.guard(length < 0, invalid)

      let limit = limits.max_body_bytes
      use <- bool.guard(length > limit, Error(BodyTooLarge(limit)))

      Ok(Declared(length))
    }
  }
}
