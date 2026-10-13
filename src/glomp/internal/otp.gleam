pub type Reason {
  Shutdown
}

@external(erlang, "erlang", "exit")
pub fn exit(reason: anything) -> never
