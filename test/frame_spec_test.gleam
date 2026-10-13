//// The codec against the STOMP 1.2 specification.

import gleam/bit_array
import gleam/int
import gleam/list
import gleam/string
import glomp/frame.{type Frame, Frame}

fn decode(bytes: BitArray) -> Result(List(Frame), frame.DecodeError) {
  decode_buffered(frame.decoder(frame.default_limits) |> frame.feed(bytes), [])
}

fn decode_buffered(
  decoder: frame.Decoder,
  frames: List(Frame),
) -> Result(List(Frame), frame.DecodeError) {
  case frame.next(decoder) {
    Error(error) -> Error(error)

    Ok(frame.Incomplete(decoder: rest)) -> {
      assert frame.next(rest) == Ok(frame.Incomplete(rest))

      Ok(list.reverse(frames))
    }

    Ok(frame.Decoded(frame: decoded, decoder:)) ->
      decode_buffered(decoder, [decoded, ..frames])
  }
}

fn text(bytes: BitArray) -> String {
  let assert Ok(text) = bit_array.to_string(bytes)

  text
}

// ------------------------------------------- the specification's own frames

pub fn spec_connect_example_test() {
  let encoded = <<
    "CONNECT\naccept-version:1.2\nhost:stomp.github.org\n\n":utf8,
    0,
  >>

  let connect =
    Frame(
      frame.Connect,
      [#("accept-version", "1.2"), #("host", "stomp.github.org")],
      <<>>,
    )

  assert decode(encoded) == Ok([connect])
  assert frame.encode(connect, frame.Stomp12) == encoded
}

pub fn spec_connected_example_test() {
  let encoded = <<"CONNECTED\nversion:1.2\n\n":utf8, 0>>
  let connected = Frame(frame.Connected, [#("version", "1.2")], <<>>)
  assert decode(encoded) == Ok([connected])
  assert frame.encode(connected, frame.Stomp12) == encoded
}

pub fn spec_send_example_test() {
  let encoded = <<
    "SEND\ndestination:/queue/a\ncontent-type:text/plain\n\nhello queue a\n":utf8,
    0,
  >>

  let send =
    Frame(
      frame.Send,
      [#("destination", "/queue/a"), #("content-type", "text/plain")],
      <<"hello queue a\n":utf8>>,
    )

  assert decode(encoded) == Ok([send])
  assert frame.encode(send, frame.Stomp12) == encoded
}

pub fn spec_subscribe_example_test() {
  let encoded = <<
    "SUBSCRIBE\nid:0\ndestination:/queue/foo\nack:client\n\n":utf8,
    0,
  >>

  let subscribe =
    Frame(
      frame.Subscribe,
      [#("id", "0"), #("destination", "/queue/foo"), #("ack", "client")],
      <<>>,
    )

  assert decode(encoded) == Ok([subscribe])
  assert frame.encode(subscribe, frame.Stomp12) == encoded
}

pub fn spec_message_example_test() {
  let encoded = <<
    "MESSAGE\nsubscription:0\nmessage-id:007\ndestination:/queue/a\ncontent-type:text/plain\n\nhello queue a":utf8,
    0,
  >>

  assert decode(encoded)
    == Ok([
      Frame(
        frame.Message,
        [
          #("subscription", "0"),
          #("message-id", "007"),
          #("destination", "/queue/a"),
          #("content-type", "text/plain"),
        ],
        <<"hello queue a":utf8>>,
      ),
    ])
}

pub fn spec_receipt_example_test() {
  assert decode(<<"RECEIPT\nreceipt-id:message-12345\n\n":utf8, 0>>)
    == Ok([Frame(frame.Receipt, [#("receipt-id", "message-12345")], <<>>)])
}

/// The spec's ERROR example has a frame as its body.
pub fn spec_error_example_test() {
  let body =
    "The message:\n-----\nMESSAGE\ndestined:/queue/a\nreceipt:message-12345\n\nHello queue a!\n-----\nDid not contain a destination header, which is REQUIRED\nfor message propagation.\n"

  let length = int.to_string(string.byte_size(body))
  // The length given in the specification.
  assert length == "170"

  let encoded = <<
    "ERROR\nreceipt-id:message-12345\ncontent-type:text/plain\ncontent-length:":utf8,
    length:utf8,
    "\nmessage:malformed frame received\n\n":utf8,
    body:utf8,
    0,
  >>

  let assert Ok([error]) = decode(encoded)
  assert error.command == frame.ServerError
  assert frame.header(error, "message") == Ok("malformed frame received")
  assert frame.header(error, "receipt-id") == Ok("message-12345")
  assert text(error.body) == body
}

pub fn spec_transaction_examples_test() {
  list.each([frame.Begin, frame.Commit, frame.Abort], fn(command) {
    let encoded = <<
      frame.command_to_string(command):utf8,
      "\ntransaction:tx1\n\n":utf8,
      0,
    >>

    let sent = Frame(command, [#("transaction", "tx1")], <<>>)
    assert decode(encoded) == Ok([sent])
    assert frame.encode(sent, frame.Stomp12) == encoded
  })
}

pub fn spec_ack_and_disconnect_examples_test() {
  assert frame.encode(
      Frame(frame.Ack, [#("id", "12345"), #("transaction", "tx1")], <<>>),
      frame.Stomp12,
    )
    == <<"ACK\nid:12345\ntransaction:tx1\n\n":utf8, 0>>

  assert frame.encode(
      Frame(frame.Unsubscribe, [#("id", "0")], <<>>),
      frame.Stomp12,
    )
    == <<"UNSUBSCRIBE\nid:0\n\n":utf8, 0>>

  assert frame.encode(
      Frame(frame.Disconnect, [#("receipt", "77")], <<>>),
      frame.Stomp12,
    )
    == <<"DISCONNECT\nreceipt:77\n\n":utf8, 0>>
}

// ------------------------------------------------------------ STOMP frames

/// Commands are case sensitive.
pub fn commands_are_exact_test() {
  list.each(
    ["Message", "message", " MESSAGE", "MESSAGE ", "MESSAGE\t", "MESS AGE", "M"],
    fn(command) {
      assert decode(<<command:utf8, "\n\n":utf8, 0>>)
        == Error(frame.UnknownCommand(command))
    },
  )
}

/// A line ends in LF, or CR then LF.
pub fn either_line_end_test() {
  let expected =
    Ok([Frame(frame.Message, [#("a", "1"), #("b", "2")], <<"x":utf8>>)])

  assert decode(<<"MESSAGE\na:1\nb:2\n\nx":utf8, 0>>) == expected
  assert decode(<<"MESSAGE\r\na:1\r\nb:2\r\n\r\nx":utf8, 0>>) == expected
  // Line endings can be mixed.
  assert decode(<<"MESSAGE\r\na:1\nb:2\r\n\nx":utf8, 0>>) == expected
  assert decode(<<"MESSAGE\na:1\r\nb:2\n\r\nx":utf8, 0>>) == expected
}

/// A carriage return in a body is data.
pub fn carriage_returns_in_a_body_test() {
  assert decode(<<"MESSAGE\n\nline one\r\nline two\r":utf8, 0>>)
    == Ok([Frame(frame.Message, [], <<"line one\r\nline two\r":utf8>>)])
}

/// "The NULL octet can be optionally followed by multiple EOLs."
pub fn line_ends_after_a_frame_test() {
  let one = <<"RECEIPT\nreceipt-id:1\n\n":utf8, 0>>
  let expected = Frame(frame.Receipt, [#("receipt-id", "1")], <<>>)

  assert decode(<<one:bits, "\n\n\n\r\n\r\n\n":utf8, one:bits, "\n":utf8>>)
    == Ok([expected, expected])
}

/// A body on any frame is returned.
pub fn any_frame_may_arrive_with_a_body_test() {
  assert decode(<<"RECEIPT\nreceipt-id:1\n\nsurprise":utf8, 0>>)
    == Ok([
      Frame(frame.Receipt, [#("receipt-id", "1")], <<"surprise":utf8>>),
    ])
}

// ----------------------------------------------------------- value encoding

/// Four escapes are defined. Others are errors.
pub fn each_escape_test() {
  let value = fn(encoded) {
    case decode(<<"MESSAGE\nk:":utf8, encoded:utf8, "\n\n":utf8, 0>>) {
      Ok([Frame(command: frame.Message, headers: [#("k", value)], body: <<>>)]) ->
        Ok(value)

      Ok(other) -> {
        assert other == []

        Error(frame.MissingTerminator)
      }

      Error(error) -> Error(error)
    }
  }

  assert value("a\\nb") == Ok("a\nb")
  assert value("a\\cb") == Ok("a:b")
  assert value("a\\\\b") == Ok("a\\b")
  assert value("a\\rb") == Ok("a\rb")
  // An escaped backslash does not begin another escape.
  assert value("\\\\n") == Ok("\\n")
  assert value("\\\\\\n") == Ok("\\\n")
  assert value("\\\\\\\\") == Ok("\\\\")
  assert value("\\c\\c\\c") == Ok(":::")
  assert value("") == Ok("")

  list.each(["t", "0", "N", "C", "R", " ", "x", "\"", "'", "/"], fn(letter) {
    assert value("a\\" <> letter <> "b")
      == Error(frame.UndefinedEscape("\\" <> letter))
  })

  assert value("ends\\") == Error(frame.UndefinedEscape("\\"))
}

/// Header names are escaped exactly as values are.
pub fn names_are_escaped_too_test() {
  assert decode(<<"MESSAGE\na\\cb\\nc\\\\d:v\n\n":utf8, 0>>)
    == Ok([Frame(frame.Message, [#("a:b\nc\\d", "v")], <<>>)])

  assert decode(<<"MESSAGE\na\\tb:v\n\n":utf8, 0>>)
    == Error(frame.UndefinedEscape("\\t"))

  assert frame.encode(
      Frame(frame.Send, [#("a:b\nc\\d", "v")], <<>>),
      frame.Stomp12,
    )
    == <<"SEND\na\\cb\\nc\\\\d:v\n\n":utf8, 0>>
}

/// CONNECT and CONNECTED are never escaped.
pub fn no_escaping_in_the_handshake_test() {
  list.each([frame.Connect, frame.Stomp, frame.Connected], fn(command) {
    let name = frame.command_to_string(command)
    let literal = Frame(command, [#("k", "a\\nb\\tc\\")], <<>>)
    assert decode(<<name:utf8, "\nk:a\\nb\\tc\\\n\n":utf8, 0>>) == Ok([literal])

    assert frame.encode(literal, frame.Stomp12)
      == <<name:utf8, "\nk:a\\nb\\tc\\\n\n":utf8, 0>>
  })
}

/// All other frames are escaped, in both directions.
pub fn every_other_frame_is_escaped_test() {
  list.each(
    [
      frame.Send, frame.Subscribe, frame.Unsubscribe, frame.Ack, frame.Nack,
      frame.Begin, frame.Commit, frame.Abort, frame.Disconnect, frame.Message,
      frame.Receipt, frame.ServerError,
    ],
    fn(command) {
      let original = Frame(command, [#("k:", "a\nb\\c:d\re")], <<>>)
      let encoded = frame.encode(original, frame.Stomp12)

      assert text(encoded)
        == frame.command_to_string(command)
        <> "\nk\\c:a\\nb\\\\c\\cd\\re\n\n\u{0}"

      assert decode(encoded) == Ok([original])
    },
  )
}

/// 1.1 has three escapes. Carriage returns are not escaped.
pub fn stomp_1_1_escapes_test() {
  assert frame.encode(
      Frame(frame.Send, [#("k", "a\nb:c\\d\re")], <<>>),
      frame.Stomp11,
    )
    == <<"SEND\nk:a\\nb\\cc\\\\d\re\n\n":utf8, 0>>
}

// ----------------------------------------------------------------- headers

/// Headers are never trimmed or padded.
pub fn spaces_are_kept_test() {
  assert decode(<<"MESSAGE\n key : value \n\ttab:\t\n\n":utf8, 0>>)
    == Ok([
      Frame(frame.Message, [#(" key ", " value "), #("\ttab", "\t")], <<>>),
    ])

  assert frame.encode(
      Frame(frame.Send, [#(" key ", " value ")], <<>>),
      frame.Stomp12,
    )
    == <<"SEND\n key : value \n\n":utf8, 0>>
}

/// When a header repeats, the first one counts.
pub fn the_first_repeat_counts_test() {
  let assert Ok([received]) =
    decode(<<"MESSAGE\nfoo:World\nfoo:Hello\nbar:1\nfoo:Again\n\n":utf8, 0>>)

  assert frame.header(received, "foo") == Ok("World")
  // All repeats are kept in `headers`.
  assert list.key_filter(received.headers, "foo") == ["World", "Hello", "Again"]
}

/// Header names are case sensitive.
pub fn names_are_case_sensitive_test() {
  let assert Ok([received]) =
    decode(<<"MESSAGE\nContent-Length:3\nDestination:/a\n\nabcdef":utf8, 0>>)

  // Not `content-length`, so the body ends at the NUL.
  assert received.body == <<"abcdef":utf8>>
  assert frame.header(received, "destination") == Error(Nil)
  assert frame.header(received, "Destination") == Ok("/a")
}

pub fn odd_but_legal_headers_test() {
  assert decode(<<"MESSAGE\nname:\na:b:c:d\n \\c:\n\n":utf8, 0>>)
    == Ok([
      Frame(frame.Message, [#("name", ""), #("a", "b:c:d"), #(" :", "")], <<>>),
    ])
}

/// A header name has at least one byte.
pub fn a_header_needs_a_name_test() {
  assert decode(<<"MESSAGE\n:value\n\n":utf8, 0>>)
    == Error(frame.MalformedHeader(":value"))

  assert decode(<<"MESSAGE\na:b\n:\n\n":utf8, 0>>)
    == Error(frame.MalformedHeader(":"))

  assert decode(<<"CONNECTED\n:x\n\n":utf8, 0>>)
    == Error(frame.MalformedHeader(":x"))
}

/// Headers can contain non-ASCII UTF-8.
pub fn utf8_headers_test() {
  let original =
    Frame(frame.Message, [#("näme", "värde ✓ 列車 🚆")], <<"bødy":utf8>>)

  assert decode(frame.encode(original, frame.Stomp12)) == Ok([original])

  assert decode(<<"MESSAGE\nk:":utf8, 0xc3, "\n\n":utf8, 0>>)
    == Error(frame.HeadNotUtf8)

  assert decode(<<0xff, "MESSAGE\n\n":utf8, 0>>) == Error(frame.HeadNotUtf8)
}

/// The first blank line ends the head.
pub fn the_first_blank_line_ends_the_head_test() {
  assert decode(<<"MESSAGE\na:1\n\nb:2\n\nrest":utf8, 0>>)
    == Ok([Frame(frame.Message, [#("a", "1")], <<"b:2\n\nrest":utf8>>)])
}

// ----------------------------------------------------------- content-length

/// With content-length, the body can contain NUL bytes.
pub fn the_length_decides_test() {
  let body = <<0, 0, 0, "MESSAGE\n\n":utf8, 0, 0>>
  let length = int.to_string(bit_array.byte_size(body))

  assert decode(<<
      "MESSAGE\ncontent-length:":utf8,
      length:utf8,
      "\n\n":utf8,
      body:bits,
      0,
    >>)
    == Ok([Frame(frame.Message, [#("content-length", length)], body)])
}

/// A body with content-length still needs a NUL after it.
pub fn the_terminator_is_still_required_test() {
  assert decode(<<"MESSAGE\ncontent-length:3\n\nabcd":utf8, 0>>)
    == Error(frame.MissingTerminator)

  assert decode(<<"MESSAGE\ncontent-length:3\n\nabc\n":utf8, 0>>)
    == Error(frame.MissingTerminator)

  assert decode(<<"MESSAGE\ncontent-length:0\n\nx":utf8, 0>>)
    == Error(frame.MissingTerminator)
}

/// Without a length, the body ends at the first NUL.
pub fn the_first_nul_ends_an_unmeasured_body_test() {
  assert decode(<<"MESSAGE\n\nab":utf8, 0, "MESSAGE\n\ncd":utf8, 0>>)
    == Ok([
      Frame(frame.Message, [], <<"ab":utf8>>),
      Frame(frame.Message, [], <<"cd":utf8>>),
    ])
}

pub fn content_length_must_be_a_count_test() {
  list.each(["", " 3", "3 ", "3.0", "0x3", "three", "-0x1", "１"], fn(value) {
    assert decode(<<
        "MESSAGE\ncontent-length:":utf8,
        value:utf8,
        "\n\nabc":utf8,
        0,
      >>)
      == Error(frame.InvalidContentLength(value))
  })

  assert decode(<<"MESSAGE\ncontent-length:-3\n\nabc":utf8, 0>>)
    == Error(frame.InvalidContentLength("-3"))
}

/// The first content-length is used.
pub fn repeated_content_length_test() {
  assert decode(<<"MESSAGE\ncontent-length:2\ncontent-length:9\n\nab":utf8, 0>>)
    == Ok([
      Frame(
        frame.Message,
        [#("content-length", "2"), #("content-length", "9")],
        <<"ab":utf8>>,
      ),
    ])
}

/// A body can contain every byte value.
pub fn every_byte_value_test() {
  let body =
    list.fold(all_bytes(255, []), <<>>, fn(body, byte) { <<body:bits, byte>> })

  assert bit_array.byte_size(body) == 256

  let encoded = <<"MESSAGE\ncontent-length:256\n\n":utf8, body:bits, 0>>

  assert decode(encoded)
    == Ok([Frame(frame.Message, [#("content-length", "256")], body)])
}

fn all_bytes(from: Int, bytes: List(Int)) -> List(Int) {
  case from < 0 {
    True -> bytes
    False -> all_bytes(from - 1, [from, ..bytes])
  }
}

// -------------------------------------------------------------- size limits

/// Limits are exact.
pub fn limits_are_exact_test() {
  let limits = frame.Limits(max_head_bytes: 12, max_body_bytes: 5)

  let next = fn(bytes) {
    frame.next(frame.decoder(limits) |> frame.feed(bytes))
  }

  let fits = fn(bytes) {
    case next(bytes) {
      Ok(frame.Decoded(frame: decoded, decoder: rest)) -> {
        assert frame.next(rest) == Ok(frame.Incomplete(rest))

        Ok(decoded.body)
      }

      Ok(frame.Incomplete(decoder: waiting)) -> {
        assert frame.next(waiting) == Ok(frame.Incomplete(waiting))

        Ok(<<"incomplete":utf8>>)
      }

      Error(error) -> Error(error)
    }
  }

  // "MESSAGE\na:bc" is twelve bytes of head.
  assert fits(<<"MESSAGE\na:bc\n\n12345":utf8, 0>>) == Ok(<<"12345":utf8>>)

  assert fits(<<"MESSAGE\na:bcd\n\n12345":utf8, 0>>)
    == Error(frame.HeadTooLarge(12))

  assert fits(<<"MESSAGE\na:bc\n\n123456":utf8, 0>>)
    == Error(frame.BodyTooLarge(5))

  assert fits(<<"SEND\nl:5\n\n12345":utf8, 0>>) == Ok(<<"12345":utf8>>)

  // A declared length over the limit fails immediately.
  let roomy = frame.Limits(max_head_bytes: 100, max_body_bytes: 5)

  let assert Ok(frame.Incomplete(decoder: waiting)) =
    frame.next(
      frame.decoder(roomy) |> frame.feed(<<"SEND\ncontent-length:5\n\n":utf8>>),
    )

  assert frame.next(waiting) == Ok(frame.Incomplete(waiting))

  assert frame.next(
      frame.decoder(roomy) |> frame.feed(<<"SEND\ncontent-length:6\n\n":utf8>>),
    )
    == Error(frame.BodyTooLarge(5))
}

/// Limits are checked on incomplete frames too.
pub fn limits_apply_while_waiting_test() {
  let limits = frame.Limits(max_head_bytes: 50, max_body_bytes: 50)

  let feed_slowly = fn(start: BitArray, piece: BitArray) {
    feed_until_refused(frame.decoder(limits) |> frame.feed(start), piece, 0)
  }

  let assert #(frame.HeadTooLarge(50), head_pieces) =
    feed_slowly(<<"MESSAGE\n":utf8>>, <<"aaaaaaaaa:b\n":utf8>>)

  assert head_pieces <= 5

  let assert #(frame.BodyTooLarge(50), body_pieces) =
    feed_slowly(<<"MESSAGE\n\n":utf8>>, <<"0123456789":utf8>>)

  assert body_pieces <= 6
}

fn feed_until_refused(
  decoder: frame.Decoder,
  piece: BitArray,
  fed: Int,
) -> #(frame.DecodeError, Int) {
  assert fed < 1000

  case frame.next(decoder) {
    Error(error) -> #(error, fed)

    Ok(frame.Incomplete(decoder: waiting)) ->
      feed_until_refused(frame.feed(waiting, piece), piece, fed + 1)

    Ok(frame.Decoded(frame: decoded, decoder: rest)) -> {
      assert decoded.headers == []

      feed_until_refused(rest, piece, fed)
    }
  }
}

/// Heart-beats do not count towards the limits.
pub fn heart_beats_are_free_test() {
  let limits = frame.Limits(max_head_bytes: 20, max_body_bytes: 20)
  let beats = string.repeat("\n", 5000) <> string.repeat("\r\n", 5000)

  let assert Ok(frame.Incomplete(decoder: waiting)) =
    frame.next(frame.decoder(limits) |> frame.feed(<<beats:utf8>>))

  let assert Ok(frame.Decoded(frame: decoded, decoder: rest)) =
    frame.next(frame.feed(waiting, <<"RECEIPT\n\n":utf8, 0>>))

  assert decoded == Frame(frame.Receipt, [], <<>>)
  assert frame.next(rest) == Ok(frame.Incomplete(rest))
}

// ---------------------------------------------------------------- the rest

/// A frame split at any byte decodes correctly.
pub fn cut_anywhere_test() {
  let original =
    Frame(
      frame.Message,
      [#("destination", "/queue/ä"), #("content-length", "7"), #("k", "✓")],
      <<"a", 0, "b\r\n", 0, "c">>,
    )

  let encoded = <<
    "\r\n":utf8,
    "MESSAGE\r\ndestination:/queue/ä\r\ncontent-length:7\r\nk:✓\r\n\r\n":utf8,
    "a":utf8,
    0,
    "b\r\n":utf8,
    0,
    "c":utf8,
    0,
    "\r\n":utf8,
  >>

  let size = bit_array.byte_size(encoded)

  list.each(all_bytes(size, []), fn(cut) {
    let assert Ok(first) = bit_array.slice(encoded, 0, cut)
    let assert Ok(second) = bit_array.slice(encoded, cut, size - cut)

    let assert Ok(early) =
      decode_buffered_keeping(
        frame.decoder(frame.default_limits) |> frame.feed(first),
        [],
      )

    let assert Ok(late) =
      decode_buffered_keeping(frame.feed(early.1, second), early.0)

    assert list.reverse(late.0) == [original]
      as { "cut at " <> int.to_string(cut) }
  })
}

fn decode_buffered_keeping(
  decoder: frame.Decoder,
  frames: List(Frame),
) -> Result(#(List(Frame), frame.Decoder), frame.DecodeError) {
  case frame.next(decoder) {
    Error(error) -> Error(error)
    Ok(frame.Incomplete(decoder: waiting)) -> Ok(#(frames, waiting))

    Ok(frame.Decoded(frame: decoded, decoder: rest)) ->
      decode_buffered_keeping(rest, [decoded, ..frames])
  }
}

/// A 1 MiB body in small chunks is scanned once.
pub fn a_large_body_in_small_packets_test() {
  let chunk = <<string.repeat("0123456789abcdef", 64):utf8>>
  let chunks = 1024

  let declared = fn(headers) {
    list.fold(
      all_bytes(chunks - 1, []),
      frame.decoder(frame.default_limits)
        |> frame.feed(<<"MESSAGE\n":utf8, headers:utf8, "\n":utf8>>),
      fn(decoder, index) {
        let assert Ok(frame.Incomplete(decoder: waiting)) = frame.next(decoder)
          as { "chunk " <> int.to_string(index) }

        frame.feed(waiting, chunk)
      },
    )
  }

  list.each(["content-length:1048576\n", ""], fn(headers) {
    let assert Ok(frame.Decoded(frame: decoded, decoder: rest)) =
      frame.next(frame.feed(declared(headers), <<0>>))

    assert bit_array.byte_size(decoded.body) == 1_048_576
    assert frame.next(rest) == Ok(frame.Incomplete(rest))
  })
}

pub fn an_error_is_final_test() {
  let decoder =
    frame.decoder(frame.default_limits) |> frame.feed(<<"NOPE\n\n":utf8, 0>>)

  assert frame.next(decoder) == Error(frame.UnknownCommand("NOPE"))
  // Calling `next` again returns the same error.
  assert frame.next(decoder) == Error(frame.UnknownCommand("NOPE"))
}

pub fn every_command_has_its_wire_name_test() {
  assert list.map(
      [
        frame.Connect, frame.Stomp, frame.Send, frame.Subscribe,
        frame.Unsubscribe, frame.Ack, frame.Nack, frame.Begin, frame.Commit,
        frame.Abort, frame.Disconnect, frame.Connected, frame.Message,
        frame.Receipt, frame.ServerError,
      ],
      frame.command_to_string,
    )
    == [
      "CONNECT", "STOMP", "SEND", "SUBSCRIBE", "UNSUBSCRIBE", "ACK", "NACK",
      "BEGIN", "COMMIT", "ABORT", "DISCONNECT", "CONNECTED", "MESSAGE",
      "RECEIPT", "ERROR",
    ]
}

pub fn the_heart_beat_is_one_line_feed_test() {
  assert frame.heart_beat == <<10>>
}

// ---------------------------------------------------------------- STOMP 1.1

fn decode_1_1(bytes: BitArray) -> Result(List(Frame), frame.DecodeError) {
  decode_buffered(
    frame.decoder(frame.default_limits)
      |> frame.set_version(frame.Stomp11)
      |> frame.feed(bytes),
    [],
  )
}

/// On 1.1, `\r` is an undefined escape.
pub fn stomp_1_1_has_no_carriage_return_escape_test() {
  assert decode_1_1(<<"MESSAGE\nk:a\\rb\n\n":utf8, 0>>)
    == Error(frame.UndefinedEscape("\\r"))

  assert decode_1_1(<<"MESSAGE\nk:a\\nb\\cc\\\\d\n\n":utf8, 0>>)
    == Ok([Frame(frame.Message, [#("k", "a\nb:c\\d")], <<>>)])
}

/// On 1.1 a carriage return before LF is kept.
pub fn stomp_1_1_keeps_carriage_returns_test() {
  assert decode_1_1(<<"MESSAGE\nk:value\r\n\n":utf8, 0>>)
    == Ok([Frame(frame.Message, [#("k", "value\r")], <<>>)])

  let original = Frame(frame.Send, [#("k", "a\rb\r"), #("j\r", "\r")], <<>>)
  assert decode_1_1(frame.encode(original, frame.Stomp11)) == Ok([original])

  // Decoded as 1.2, carriage returns before line feeds are removed.
  assert decode(frame.encode(original, frame.Stomp11))
    == Ok([Frame(frame.Send, [#("k", "a\rb"), #("j\r", "")], <<>>)])
}

/// A 1.1 decoder does not read CRLF line ends.
pub fn stomp_1_1_does_not_read_crlf_test() {
  assert decode_1_1(<<"MESSAGE\r\nk:v\r\n\r\n":utf8, 0>>) == Ok([])

  assert decode_1_1(<<"MESSAGE\r\nk:v\n\n":utf8, 0>>)
    == Error(frame.UnknownCommand("MESSAGE\r"))

  // On 1.1 only a line feed is a heart-beat.
  assert decode_1_1(<<"\n\n\nRECEIPT\n\n":utf8, 0, "\n":utf8>>)
    == Ok([Frame(frame.Receipt, [], <<>>)])

  assert decode_1_1(<<"\r\nRECEIPT\n\n":utf8, 0>>)
    == Error(frame.UnknownCommand("\r"))
}

/// A version change applies from the next frame.
pub fn the_version_can_change_between_frames_test() {
  let encoded = <<
    "CONNECTED\r\nversion:1.1\r\n\r\n":utf8,
    0,
    "MESSAGE\nk:v\r\n\n":utf8,
    0,
  >>

  let assert Ok(frame.Decoded(frame: connected, decoder:)) =
    frame.next(frame.decoder(frame.default_limits) |> frame.feed(encoded))

  assert connected == Frame(frame.Connected, [#("version", "1.1")], <<>>)

  let assert Ok(frame.Decoded(frame: message, decoder: rest)) =
    frame.next(frame.set_version(decoder, frame.Stomp11))

  assert message == Frame(frame.Message, [#("k", "v\r")], <<>>)
  assert frame.next(rest) == Ok(frame.Incomplete(rest))
}
