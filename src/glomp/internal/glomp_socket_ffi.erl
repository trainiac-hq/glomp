-module(glomp_socket_ffi).
-export([
    send/2,
    recv/2,
    set_active_once/1,
    set_passive/1,
    hand_over/2,
    close/1,
    take_delivered/1,
    incoming/1,
    start_ssl/0,
    system_certificates/0,
    network_error/1
]).

send({tcp, Socket}, Data) -> to_result(gen_tcp:send(Socket, Data));
send({ssl, Socket}, Data) -> to_result(ssl:send(Socket, Data)).

recv({tcp, Socket}, Timeout) -> to_result(gen_tcp:recv(Socket, 0, Timeout));
recv({ssl, Socket}, Timeout) -> to_result(ssl:recv(Socket, 0, Timeout)).

set_active_once({tcp, Socket}) -> options_set(inet:setopts(Socket, [{active, once}]));
set_active_once({ssl, Socket}) -> options_set(ssl:setopts(Socket, [{active, once}])).

set_passive({tcp, Socket}) -> options_set(inet:setopts(Socket, [{active, false}]));
set_passive({ssl, Socket}) -> options_set(ssl:setopts(Socket, [{active, false}])).

%% Setting options on a closed socket fails with einval.
options_set({error, einval}) -> {error, closed};
options_set(Result) -> to_result(Result).

hand_over({tcp, Socket}, Pid) -> to_result(gen_tcp:controlling_process(Socket, Pid));
hand_over({ssl, Socket}, Pid) -> to_result(ssl:controlling_process(Socket, Pid)).

close({tcp, Socket}) -> to_nil(gen_tcp:close(Socket));
close({ssl, Socket}) -> to_nil(ssl:close(Socket)).

%% Take bytes the socket already delivered to this mailbox.
take_delivered(Socket) ->
    take_delivered(Socket, <<>>).

take_delivered({Tag, Handle} = Socket, Taken) ->
    receive
        {Tag, Handle, Data} -> take_delivered(Socket, <<Taken/binary, Data/binary>>)
    after 0 ->
        Taken
    end.

%% Convert a socket message to a `socket.Incoming`.
incoming({tcp, Socket, Data}) -> {incoming, {tcp, Socket}, {arrived, Data}};
incoming({ssl, Socket, Data}) -> {incoming, {ssl, Socket}, {arrived, Data}};
incoming({tcp_closed, Socket}) -> {incoming, {tcp, Socket}, ended};
incoming({ssl_closed, Socket}) -> {incoming, {ssl, Socket}, ended};
incoming({tcp_error, Socket, Reason}) -> {incoming, {tcp, Socket}, {errored, network_error(Reason)}};
incoming({ssl_error, Socket, Reason}) -> {incoming, {ssl, Socket}, {errored, network_error(Reason)}}.

start_ssl() ->
    case application:ensure_all_started(ssl) of
        {ok, Started} when is_list(Started) -> {ok, nil};
        {error, Reason} -> {error, {tls_not_available, Reason}}
    end.

%% `public_key:cacerts_get/0` raises an error when the system has none.
system_certificates() ->
    try
        {ok, public_key:cacerts_get()}
    catch
        error:Reason -> {error, {no_system_certificates, Reason}}
    end.

to_result(ok) -> {ok, nil};
to_result({ok, Value}) -> {ok, Value};
to_result({error, Reason}) -> {error, failure(Reason)}.

%% Convert an error term to a `socket.Failure`.
failure(closed) -> closed;
failure(timeout) -> timed_out;
failure(Reason) -> {failed, network_error(Reason)}.

to_nil(ok) -> nil;
to_nil({error, Reason}) when is_atom(Reason); is_tuple(Reason) -> nil.

%% Convert an error term to a `network.NetworkError`.
network_error(closed) -> connection_closed;
network_error(econnrefused) -> connection_refused;
network_error(econnreset) -> connection_reset;
network_error(econnaborted) -> connection_aborted;
network_error(timeout) -> timed_out;
network_error(etimedout) -> connection_timed_out;
network_error(enotconn) -> socket_not_connected;
network_error(epipe) -> broken_pipe;
network_error(nxdomain) -> host_not_found;
network_error(ehostdown) -> host_down;
network_error(ehostunreach) -> host_unreachable;
network_error(enetdown) -> network_down;
network_error(enetunreach) -> network_unreachable;
network_error(eaddrinuse) -> address_in_use;
network_error(eaddrnotavail) -> address_not_available;
network_error(eafnosupport) -> address_family_not_supported;
network_error(emfile) -> too_many_open_files;
network_error(enfile) -> system_file_table_full;
network_error(enobufs) -> no_buffer_space;
network_error(enomem) -> out_of_memory;
network_error(eacces) -> permission_denied;
network_error(eperm) -> operation_not_permitted;
network_error(einval) -> invalid_argument;
network_error({tls_alert, {Alert, Text}}) when is_atom(Alert), is_list(Text) ->
    {tls_alert, tls_alert(Alert), unicode:characters_to_binary(Text)};
network_error({options, {cacertfile, Path, Reason}}) when is_list(Path); is_binary(Path) ->
    {certificate_file_unreadable, unicode:characters_to_binary(Path), Reason};
network_error({options, Details}) ->
    {invalid_tls_options, Details};
network_error(Unknown) ->
    {unknown_network_error, Unknown}.

%% Erlang spells these two the American way.
tls_alert(user_canceled) -> user_cancelled;
tls_alert(unrecognized_name) -> unrecognised_name;
tls_alert(Alert) ->
    Known = [
        close_notify,
        unexpected_message,
        bad_record_mac,
        record_overflow,
        handshake_failure,
        bad_certificate,
        unsupported_certificate,
        certificate_revoked,
        certificate_expired,
        certificate_unknown,
        illegal_parameter,
        unknown_ca,
        access_denied,
        decode_error,
        decrypt_error,
        protocol_version,
        insufficient_security,
        internal_error,
        inappropriate_fallback,
        no_renegotiation,
        missing_extension,
        unsupported_extension,
        bad_certificate_status_response,
        unknown_psk_identity,
        certificate_required,
        no_application_protocol
    ],

    case lists:member(Alert, Known) of
        true -> Alert;
        false -> {unknown_alert, atom_to_binary(Alert)}
    end.
