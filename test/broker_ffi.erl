-module(broker_ffi).
-export([listen/0, listen_tls/0, accept/2, recv/2, send/2, close/1, stop/1, free_port/0, env/1, only_child/1, linked_processes/2, try_send/3, reset/1]).

listen() ->
    {ok, Listener} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(Listener),
    {{tcp, Listener}, Port}.

listen_tls() ->
    {ok, Started} = application:ensure_all_started(ssl),
    true = is_list(Started),
    #{server_config := Server, client_config := Client} =
        public_key:pkix_test_data(#{
            server_chain => #{
                root => strong(),
                intermediates => [],
                peer => [{extensions, [san()]} | strong()]
            },
            client_chain => #{root => strong(), intermediates => [], peer => strong()}
        }),
    {cacerts, Authorities} = lists:keyfind(cacerts, 1, Client),
    Pem = public_key:pem_encode([{'Certificate', Der, not_encrypted} || Der <- Authorities]),
    Path = filename:join(
        filename:basedir(user_cache, "glomp-test"),
        integer_to_list(erlang:unique_integer([positive])) ++ ".pem"
    ),
    ok = filelib:ensure_dir(Path),
    ok = file:write_file(Path, Pem),
    {ok, Listener} = ssl:listen(0, [binary, {active, false}, {reuseaddr, true} | Server]),
    {ok, {Address, Port}} = ssl:sockname(Listener),
    true = is_tuple(Address),
    {{ssl, Listener}, Port, unicode:characters_to_binary(Path)}.

strong() ->
    [{key, {rsa, 2048, 65537}}, {digest, sha256}].

san() ->
    {'Extension', {2, 5, 29, 17}, false, [{dNSName, "localhost"}]}.

accept({tcp, Listener}, Timeout) ->
    case gen_tcp:accept(Listener, Timeout) of
        {ok, Socket} -> {ok, {tcp, Socket}};
        {error, Reason} when is_atom(Reason) -> {error, nil}
    end;
accept({ssl, Listener}, Timeout) ->
    case ssl:transport_accept(Listener, Timeout) of
        {ok, Transport} ->
            case ssl:handshake(Transport, Timeout) of
                {ok, Socket} -> {ok, {ssl, Socket}};
                {error, Reason} when is_atom(Reason); is_tuple(Reason) -> {error, nil}
            end;
        {error, Reason} when is_atom(Reason) -> {error, nil}
    end.

recv({tcp, Socket}, Timeout) -> received(gen_tcp:recv(Socket, 0, Timeout));
recv({ssl, Socket}, Timeout) -> received(ssl:recv(Socket, 0, Timeout)).

received({ok, Data}) -> {ok, Data};
received({error, timeout}) -> {error, timed_out};
received({error, Reason}) when is_atom(Reason) -> {error, closed}.

send({tcp, Socket}, Data) ->
    ok = gen_tcp:send(Socket, Data),
    nil;
send({ssl, Socket}, Data) ->
    ok = ssl:send(Socket, Data),
    nil.

close({tcp, Socket}) ->
    ok = gen_tcp:close(Socket),
    nil;
close({ssl, Socket}) ->
    ok = ssl:close(Socket),
    nil.

stop(Listener) -> close(Listener).

%% Finds an available port
free_port() ->
    {{tcp, Listener}, Port} = listen(),
    ok = gen_tcp:close(Listener),
    Port.

env(Name) ->
    case os:getenv(unicode:characters_to_list(Name)) of
        false -> {error, nil};
        Value -> {ok, unicode:characters_to_binary(Value)}
    end.

only_child(Supervisor) ->
    [{Id, Child, Type, Modules}] = supervisor:which_children(Supervisor),
    true = is_pid(Child) andalso is_atom(Type) andalso Id =/= Modules,
    Child.

%% Whether Data was written within Timeout
try_send({tcp, Socket}, Data, Timeout) ->
    ok = inet:setopts(Socket, [{send_timeout, Timeout}]),
    case gen_tcp:send(Socket, Data) of
        ok -> true;
        {error, timeout} -> false
    end.

reset({tcp, Socket}) ->
    ok = inet:setopts(Socket, [{linger, {true, 0}}]),
    ok = gen_tcp:close(Socket),
    nil.

%% The processes linked to Pid, without Except
linked_processes(Pid, Except) ->
    {links, Links} = process_info(Pid, links),
    [Linked || Linked <- Links, is_pid(Linked), Linked =/= Except].
