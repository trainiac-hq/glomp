//// Finding and cutting byte strings. The frame decoder uses them to
//// find where headers and bodies end, and the client uses them too.

import gleam/bit_array
import gleam/result

/// Find the first pattern at or after `from`. Returns its position and
/// length.
@external(erlang, "glomp_bytes_ffi", "find")
pub fn find(
  data: BitArray,
  patterns: List(BitArray),
  from: Int,
) -> Result(#(Int, Int), Nil)

@external(erlang, "binary", "part")
pub fn slice(data: BitArray, start: Int, length: Int) -> BitArray

pub fn drop(data: BitArray, count: Int) -> BitArray {
  slice(data, count, bit_array.byte_size(data) - count)
}

pub fn has_nul(data: BitArray) -> Bool {
  find(data, [<<0>>], 0) |> result.is_ok
}
