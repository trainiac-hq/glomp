import gleam/bit_array
import gleam/list
import glomp/frame.{Frame}

fn decode_all(
  decoder: frame.Decoder,
  frames: List(frame.Frame),
) -> Result(#(List(frame.Frame), frame.Decoder), frame.DecodeError) {
  case frame.next(decoder) {
    Error(error) -> Error(error)
    Ok(frame.Incomplete(decoder:)) -> Ok(#(list.reverse(frames), decoder))

    Ok(frame.Decoded(frame: decoded, decoder:)) ->
      decode_all(decoder, [decoded, ..frames])
  }
}

/// Decode all frames in `bytes`, fed at once.
fn decode(bytes: BitArray) -> Result(List(frame.Frame), frame.DecodeError) {
  case
    decode_all(frame.decoder(frame.default_limits) |> frame.feed(bytes), [])
  {
    Ok(#(frames, rest)) -> {
      // No bytes remain in the decoder.
      assert frame.next(rest) == Ok(frame.Incomplete(rest))

      Ok(frames)
    }

    Error(error) -> Error(error)
  }
}

/// Decode all frames in `bytes`, one byte at a time.
fn decode_byte_by_byte(
  bytes: BitArray,
  decoder: frame.Decoder,
  frames: List(frame.Frame),
) -> List(frame.Frame) {
  case bytes {
    <<byte, rest:bytes>> -> {
      let assert Ok(#(decoded, decoder)) =
        decode_all(frame.feed(decoder, <<byte>>), [])

      decode_byte_by_byte(rest, decoder, list.append(frames, decoded))
    }

    empty -> {
      assert empty == <<>>

      frames
    }
  }
}

pub fn encode_test() {
  let encoded =
    Frame(frame.Send, [#("destination", "/queue/a"), #("content-length", "5")], <<
      "hello":utf8,
    >>)
    |> frame.encode(frame.Stomp12)

  assert encoded
    == <<"SEND\ndestination:/queue/a\ncontent-length:5\n\nhello":utf8, 0>>
}

pub fn encode_without_headers_test() {
  assert frame.encode(Frame(frame.Disconnect, [], <<>>), frame.Stomp12)
    == <<"DISCONNECT\n\n":utf8, 0>>
}

pub fn encode_escapes_headers_test() {
  let tricky = Frame(frame.Send, [#("a:b", "one\ntwo\\three\rfour")], <<>>)

  assert frame.encode(tricky, frame.Stomp12)
    == <<"SEND\na\\cb:one\\ntwo\\\\three\\rfour\n\n":utf8, 0>>

  // 1.1 has no escape for a carriage return.
  assert frame.encode(tricky, frame.Stomp11)
    == <<"SEND\na\\cb:one\\ntwo\\\\three\rfour\n\n":utf8, 0>>
}

/// CONNECT and CONNECTED are never escaped.
pub fn connect_frames_are_not_escaped_test() {
  assert frame.encode(
      Frame(frame.Connect, [#("passcode", "a\\b:c")], <<>>),
      frame.Stomp12,
    )
    == <<"CONNECT\npasscode:a\\b:c\n\n":utf8, 0>>

  assert decode(<<"CONNECTED\nserver:a\\b:c\n\n":utf8, 0>>)
    == Ok([Frame(frame.Connected, [#("server", "a\\b:c")], <<>>)])
}

pub fn every_command_round_trips_test() {
  let commands = [
    frame.Connect, frame.Stomp, frame.Send, frame.Subscribe, frame.Unsubscribe,
    frame.Ack, frame.Nack, frame.Begin, frame.Commit, frame.Abort,
    frame.Disconnect, frame.Connected, frame.Message, frame.Receipt,
    frame.ServerError,
  ]

  list.each(commands, fn(command) {
    let original = Frame(command, [#("k", "v")], <<"body":utf8>>)
    assert decode(frame.encode(original, frame.Stomp12)) == Ok([original])
  })
}

pub fn escapes_round_trip_test() {
  let original =
    Frame(frame.Message, [#("we:ird\n", "a\\b\r\nc:d"), #("plain", "")], <<>>)

  assert decode(frame.encode(original, frame.Stomp12)) == Ok([original])
}

/// Packets can contain several frames, or part of one.
pub fn frames_arrive_in_any_pieces_test() {
  let frames = [
    Frame(frame.Message, [#("message-id", "1")], <<"first":utf8>>),
    Frame(frame.Message, [#("content-length", "3")], <<0, 1, 0>>),
    Frame(frame.Receipt, [#("receipt-id", "r")], <<>>),
  ]

  let bytes =
    list.map(frames, frame.encode(_, frame.Stomp12)) |> bit_array.concat

  assert decode(bytes) == Ok(frames)
  assert decode_byte_by_byte(bytes, frame.decoder(frame.default_limits), [])
    == frames
}

pub fn heart_beats_between_frames_are_skipped_test() {
  let one = frame.encode(Frame(frame.Receipt, [], <<>>), frame.Stomp12)
  let bytes = <<"\n\r\n\n":utf8, one:bits, "\n\n":utf8, one:bits, "\r\n":utf8>>

  let expected = [
    Frame(frame.Receipt, [], <<>>),
    Frame(frame.Receipt, [], <<>>),
  ]

  assert decode(bytes) == Ok(expected)
  assert decode_byte_by_byte(bytes, frame.decoder(frame.default_limits), [])
    == expected
}

pub fn only_heart_beats_is_no_frame_test() {
  assert decode(<<"\n\n\r\n":utf8>>) == Ok([])
  assert decode(frame.heart_beat) == Ok([])
}

/// 1.2 allows CRLF line ends.
pub fn crlf_line_ends_test() {
  let bytes = <<"MESSAGE\r\nmessage-id:7\r\nempty:\r\n\r\nbody":utf8, 0>>

  let expected = [
    Frame(frame.Message, [#("message-id", "7"), #("empty", "")], <<
      "body":utf8,
    >>),
  ]

  assert decode(bytes) == Ok(expected)
  assert decode_byte_by_byte(bytes, frame.decoder(frame.default_limits), [])
    == expected
}

/// With content-length a body can contain NUL bytes.
pub fn content_length_test() {
  let body = <<"ab":utf8, 0, "cd":utf8>>

  assert decode(<<"MESSAGE\ncontent-length:5\n\n":utf8, body:bits, 0>>)
    == Ok([Frame(frame.Message, [#("content-length", "5")], body)])

  assert decode(<<"MESSAGE\ncontent-length:0\n\n":utf8, 0>>)
    == Ok([Frame(frame.Message, [#("content-length", "0")], <<>>)])
}

pub fn waits_for_the_whole_frame_test() {
  assert decode(<<"MESSAGE\nid:1\n":utf8>>) == Ok([])
  assert decode(<<"MESSAGE\nid:1\n\nno terminator yet":utf8>>) == Ok([])
  assert decode(<<"MESSAGE\ncontent-length:10\n\nshort":utf8>>) == Ok([])
  // Ten body bytes, without the NUL.
  assert decode(<<"MESSAGE\ncontent-length:10\n\n0123456789":utf8>>) == Ok([])
}

pub fn first_header_wins_test() {
  let assert Ok([received]) = decode(<<"MESSAGE\na:1\na:2\n\n":utf8, 0>>)
  assert received.headers == [#("a", "1"), #("a", "2")]
  assert frame.header(received, "a") == Ok("1")
  assert frame.header(received, "b") == Error(Nil)
}

pub fn a_value_may_hold_colons_test() {
  assert decode(<<"MESSAGE\nurl:http://x:1\n\n":utf8, 0>>)
    == Ok([Frame(frame.Message, [#("url", "http://x:1")], <<>>)])
}

pub fn malformed_frames_test() {
  assert decode(<<"HELLO\n\n":utf8, 0>>) == Error(frame.UnknownCommand("HELLO"))

  assert decode(<<"message\n\n":utf8, 0>>)
    == Error(frame.UnknownCommand("message"))

  assert decode(<<"MESSAGE\nno colon\n\n":utf8, 0>>)
    == Error(frame.MalformedHeader("no colon"))

  assert decode(<<"MESSAGE\na:b\\tc\n\n":utf8, 0>>)
    == Error(frame.UndefinedEscape("\\t"))

  assert decode(<<"MESSAGE\na:b\\\n\n":utf8, 0>>)
    == Error(frame.UndefinedEscape("\\"))

  assert decode(<<"MESSAGE\ncontent-length:five\n\n":utf8, 0>>)
    == Error(frame.InvalidContentLength("five"))

  assert decode(<<"MESSAGE\ncontent-length:-1\n\n":utf8, 0>>)
    == Error(frame.InvalidContentLength("-1"))

  assert decode(<<"MESSAGE\ncontent-length:2\n\nabc":utf8, 0>>)
    == Error(frame.MissingTerminator)

  assert decode(<<"MESSAGE\na:":utf8, 0xff, "\n\n":utf8, 0>>)
    == Error(frame.HeadNotUtf8)
}

pub fn limits_test() {
  let limits = frame.Limits(max_head_bytes: 20, max_body_bytes: 4)

  let decode = fn(bytes) {
    frame.next(frame.decoder(limits) |> frame.feed(bytes))
  }

  let assert Ok(frame.Decoded(frame: fits, decoder: rest)) =
    decode(<<"MESSAGE\na:b\n\n1234":utf8, 0>>)

  assert fits == Frame(frame.Message, [#("a", "b")], <<"1234":utf8>>)
  assert frame.next(rest) == Ok(frame.Incomplete(rest))

  assert decode(<<"MESSAGE\na:b\n\n12345":utf8, 0>>)
    == Error(frame.BodyTooLarge(4))

  // Fails before the NUL arrives.
  assert decode(<<"MESSAGE\na:b\n\n12345":utf8>>)
    == Error(frame.BodyTooLarge(4))

  // Fails on the head, before the body arrives.
  assert decode(<<"MESSAGE\ncontent-length:5\n\n":utf8>>)
    == Error(frame.HeadTooLarge(20))

  assert decode(<<"MESSAGE\naaaaaaaaaaaa:b\n\n":utf8, 0>>)
    == Error(frame.HeadTooLarge(20))

  assert decode(<<"MESSAGE\naaaaaaaaaaaaaaaaaaaaaaaa":utf8>>)
    == Error(frame.HeadTooLarge(20))
}

pub fn declared_body_over_the_limit_test() {
  let limits = frame.Limits(max_head_bytes: 100, max_body_bytes: 4)

  assert frame.next(
      frame.decoder(limits)
      |> frame.feed(<<"MESSAGE\ncontent-length:5\n\n":utf8>>),
    )
    == Error(frame.BodyTooLarge(4))
}

pub fn command_to_string_test() {
  assert frame.command_to_string(frame.ServerError) == "ERROR"
  assert frame.command_to_string(frame.Send) == "SEND"
}
