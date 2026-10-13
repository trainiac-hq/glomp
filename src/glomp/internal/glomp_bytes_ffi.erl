-module(glomp_bytes_ffi).
-export([find/3]).

find(Data, Patterns, From) ->
    Scope = {From, byte_size(Data) - From},

    case binary:match(Data, Patterns, [{scope, Scope}]) of
        {Position, Length} -> {ok, {Position, Length}};
        nomatch -> {error, nil}
    end.
