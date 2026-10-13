//// The codec against thousands of seeded random frames.

import gleam/bit_array
import gleam/int
import gleam/list
import gleam/result
import gleam/string
import glomp/frame.{type Frame, Frame}

/// Returns the next seed and a number below `below`.
fn random_below(seed: Int, below: Int) -> #(Int, Int) {
  let next = { seed * 1_103_515_245 + 12_345 } % 2_147_483_648

  #(next, { next / 65_536 } % below)
}

const commands = [
  frame.Connect,
  frame.Stomp,
  frame.Send,
  frame.Subscribe,
  frame.Unsubscribe,
  frame.Ack,
  frame.Nack,
  frame.Begin,
  frame.Commit,
  frame.Abort,
  frame.Disconnect,
  frame.Connected,
  frame.Message,
  frame.Receipt,
  frame.ServerError,
]

/// Fragments for header names and values, including special bytes.
const fragments = [
  "a", "b", "key", "value", " ", "", "\\", ":", "\n", "\r", "\r\n", "\\n", "\\c",
  "\\\\", "ü", "✓", "🚆", "0", "content", "-", "/queue/a", "\t",
]

fn choose(seed: Int, from: List(a), otherwise: a) -> #(Int, a) {
  let #(seed, index) = random_below(seed, list.length(from))

  #(seed, list.first(list.drop(from, index)) |> result.unwrap(otherwise))
}

fn text(seed: Int, pieces: Int, so_far: String) -> #(Int, String) {
  case pieces <= 0 {
    True -> #(seed, so_far)

    False -> {
      let #(seed, piece) = choose(seed, fragments, "")

      text(seed, pieces - 1, so_far <> piece)
    }
  }
}

fn bytes(seed: Int, count: Int, so_far: BitArray) -> #(Int, BitArray) {
  case count <= 0 {
    True -> #(seed, so_far)

    False -> {
      // One byte in four is a NUL or line end.
      let #(seed, kind) = random_below(seed, 4)

      let #(seed, byte) = case kind == 0 {
        True -> choose(seed, [0, 10, 13], 0)
        False -> random_below(seed, 256)
      }

      bytes(seed, count - 1, <<so_far:bits, byte>>)
    }
  }
}

fn headers(
  seed: Int,
  count: Int,
  so_far: List(#(String, String)),
) -> #(Int, List(#(String, String))) {
  case count <= 0 {
    True -> #(seed, so_far)

    False -> {
      let #(seed, name_pieces) = random_below(seed, 4)
      let #(seed, name) = text(seed, name_pieces, "")
      let #(seed, value_pieces) = random_below(seed, 5)
      let #(seed, value) = text(seed, value_pieces, "")

      // Names are not empty, and never `content-length`.
      let name = case name == "" || name == "content-length" {
        True -> "n" <> name
        False -> name
      }

      headers(seed, count - 1, [#(name, value), ..so_far])
    }
  }
}

/// A random frame with the right content-length.
fn random_frame(seed: Int) -> #(Int, Frame) {
  let #(seed, command) = choose(seed, commands, frame.Send)
  let #(seed, header_count) = random_below(seed, 6)
  let #(seed, headers) = headers(seed, header_count, [])
  let #(seed, body_size) = random_below(seed, 40)
  let #(seed, body) = bytes(seed, body_size, <<>>)
  let length = #("content-length", int.to_string(bit_array.byte_size(body)))

  #(seed, Frame(command, [length, ..headers], body))
}

/// Whether a frame's headers can be encoded and decoded unchanged.
fn sendable(candidate: Frame, version: frame.Version) -> Bool {
  let plain = fn(text) {
    !string.contains(text, "\n") && !string.contains(text, "\r")
  }

  let unescaped = case candidate.command {
    frame.Connect | frame.Stomp | frame.Connected -> True

    frame.Send
    | frame.Subscribe
    | frame.Unsubscribe
    | frame.Ack
    | frame.Nack
    | frame.Begin
    | frame.Commit
    | frame.Abort
    | frame.Disconnect
    | frame.Message
    | frame.Receipt
    | frame.ServerError -> False
  }

  list.all(candidate.headers, fn(header) {
    case unescaped, version {
      True, frame.Stomp11 | True, frame.Stomp12 ->
        plain(header.0) && plain(header.1) && !string.contains(header.0, ":")

      // On 1.1 carriage returns are part of the value.
      False, frame.Stomp11 | False, frame.Stomp12 -> True
    }
  })
}

fn decode_buffered(
  decoder: frame.Decoder,
  frames: List(Frame),
) -> Result(#(List(Frame), frame.Decoder), frame.DecodeError) {
  case frame.next(decoder) {
    Error(error) -> Error(error)
    Ok(frame.Incomplete(decoder: waiting)) -> Ok(#(frames, waiting))

    Ok(frame.Decoded(frame: decoded, decoder: rest)) ->
      decode_buffered(rest, [decoded, ..frames])
  }
}

/// Feed `encoded` in chunks of random size.
fn feed_in_pieces(
  seed: Int,
  encoded: BitArray,
  decoder: frame.Decoder,
  frames: List(Frame),
) -> Result(#(Int, List(Frame)), frame.DecodeError) {
  let size = bit_array.byte_size(encoded)

  case size {
    0 -> Ok(#(seed, list.reverse(frames)))

    remaining -> {
      let #(seed, cut) = random_below(seed, int.min(remaining, 24))
      let cut = cut + 1
      let assert Ok(piece) = bit_array.slice(encoded, 0, cut)
      let assert Ok(rest) = bit_array.slice(encoded, cut, remaining - cut)

      case decode_buffered(frame.feed(decoder, piece), frames) {
        Ok(#(frames, decoder)) -> feed_in_pieces(seed, rest, decoder, frames)
        Error(error) -> Error(error)
      }
    }
  }
}

fn round_trips(seed: Int, rounds: Int, version: frame.Version) -> Nil {
  case rounds <= 0 {
    True -> Nil

    False -> {
      let started = seed
      // Several consecutive frames, some followed by heart-beats.
      let #(seed, count) = random_below(seed, 4)

      let #(seed, frames, encoded) =
        list.fold(
          list.repeat(Nil, count + 1),
          #(seed, [], <<>>),
          fn(so_far, nil) {
            let #(seed, candidate) = random_frame(so_far.0)
            let #(seed, beats) = random_below(seed, 3)

            case sendable(candidate, version), nil {
              False, Nil -> #(seed, so_far.1, so_far.2)

              True, Nil -> #(seed, [candidate, ..so_far.1], <<
                so_far.2:bits,
                frame.encode(candidate, version):bits,
                string.repeat("\n", beats):utf8,
              >>)
            }
          },
        )

      let decoder =
        frame.decoder(frame.default_limits) |> frame.set_version(version)

      let assert Ok(#(seed, decoded)) =
        feed_in_pieces(seed, encoded, decoder, [])
        as { "seed " <> int.to_string(started) }

      assert decoded == list.reverse(frames)
        as { "seed " <> int.to_string(started) }

      round_trips(seed, rounds - 1, version)
    }
  }
}

/// Encoded frames decode to the same frames, in any chunks.
pub fn random_frames_round_trip_on_1_2_test() {
  round_trips(1, 3000, frame.Stomp12)
}

pub fn random_frames_round_trip_on_1_1_test() {
  round_trips(2, 3000, frame.Stomp11)
}

/// Random bytes never crash the decoder.
pub fn random_bytes_never_crash_test() {
  random_bytes_rounds(3, 4000)
}

fn random_bytes_rounds(seed: Int, rounds: Int) -> Nil {
  case rounds <= 0 {
    True -> Nil

    False -> {
      let started = seed
      let #(seed, size) = random_below(seed, 200)
      let #(seed, noise) = bytes(seed, size, <<>>)
      let limits = frame.Limits(max_head_bytes: 64, max_body_bytes: 64)
      let outcome = feed_in_pieces(seed, noise, frame.decoder(limits), [])

      let seed = case outcome {
        Ok(#(seed, frames)) -> {
          // Decoded bodies are within the limits.
          assert list.all(frames, fn(read) {
            bit_array.byte_size(read.body) <= 64
          })
            as { "seed " <> int.to_string(started) }

          seed
        }

        Error(error) -> {
          assert error != frame.InvalidContentLength("unreachable")

          seed + 1
        }
      }

      random_bytes_rounds(seed, rounds - 1)
    }
  }
}

/// Frames with one byte changed never crash the decoder.
pub fn corrupted_frames_never_crash_test() {
  corrupt_rounds(4, 4000)
}

fn corrupt_rounds(seed: Int, rounds: Int) -> Nil {
  case rounds <= 0 {
    True -> Nil

    False -> {
      let #(seed, original) = random_frame(seed)
      let encoded = frame.encode(original, frame.Stomp12)
      let size = bit_array.byte_size(encoded)
      let #(seed, at) = random_below(seed, size)
      let #(seed, byte) = random_below(seed, 256)
      let assert Ok(before) = bit_array.slice(encoded, 0, at)
      let assert Ok(after) = bit_array.slice(encoded, at + 1, size - at - 1)

      let seed = case
        feed_in_pieces(
          seed,
          <<before:bits, byte, after:bits>>,
          frame.decoder(frame.default_limits),
          [],
        )
      {
        Ok(read) -> read.0

        Error(error) -> {
          assert error != frame.InvalidContentLength("unreachable")

          seed + 1
        }
      }

      corrupt_rounds(seed, rounds - 1)
    }
  }
}
