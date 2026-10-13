//// The current time in milliseconds, for timeouts and heart-beats. It uses
//// the VM's monotonic clock, which only moves forwards, so changes to the
//// system clock leave timeouts correct.

type Unit {
  Millisecond
}

pub fn now_ms() -> Int {
  monotonic_time(Millisecond)
}

@external(erlang, "erlang", "monotonic_time")
fn monotonic_time(unit: Unit) -> Int
