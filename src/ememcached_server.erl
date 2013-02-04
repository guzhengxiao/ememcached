-module(ememcached_server).

-export([start_link/0 , start/0, init/0]).
-export([loop/1,exec_recv/1]).

-record(state, {}).

-record(ememcached, {key, value ,flag, expr , ver}).
-record(ememcached_ver , {k,v}).

start_link()->
    init().

start()->
    init().

init() ->
    Tables = mnesia:system_info(tables)
    ,DbNodes = mnesia:system_info(db_nodes)
    ,case lists:member(ememecached , Tables) of
        false -> init_table(ememecached,DbNodes)
        ;_ -> ok
    end
    ,case lists:member(ememecached_ver , Tables) of
        false -> init_table(ememecached_ver,DbNodes)
        ;_ -> ok
    end
    ,{ok, Port} = application:get_env(ememecached, memcached_port)
    ,{ok, ListenSocket} = gen_tcp:listen(Port, [{packet, line}, {reuseaddr, true}, binary])
    ,Pid = spawn_link(?MODULE, loop, [ListenSocket])
    ,register( ?MODULE ,Pid)
    ,{ok, Pid}.


init_table(ememcached,Nodes) ->
    mnesia:create_table(ememcached , [
                                     {type, ordered_set},
                                     {disc_copies, Nodes},
                                     {attributes, record_info(fields,
                                             ememcached)}
                                    ])
;init_table(ememcached_ver,Nodes) ->
    mnesia:create_table(ememcached_ver , [
                                     {type, set},
                                     {disc_copies, Nodes},
                                     {attributes, record_info(fields,
                                             ememcached_ver)}
                                    ]).

loop(ListenSocket) ->
    {ok, Socket} = gen_tcp:accept(ListenSocket)
    ,inet:setopts(Socket, [{active, true}])
    ,Pid = spawn(?MODULE, exec_recv, [Socket])
    ,gen_tcp:controlling_process(Socket, Pid)
    ,loop(ListenSocket).

exec_recv(Socket) ->
    receive
        {tcp , Socket , Msg} ->
            try
                do(Msg , Socket)
            catch
                throw:error -> gen_tcp:send( Socket , <<"ERROR\r\n">> )
                ;throw:exists -> gen_tcp:send(Socket , <<"EXISTS\r\n">> )
                ;throw:not_found -> gen_tcp:send(Socket , <<"NOT_FOUND\r\n">> )
                ;_:_ -> 
                    gen_tcp:send( Socket , <<"ERROR\r\n">> )
            end
            ,exec_recv(Socket)
        ;{tcp_closed, Socket} -> gen_tcp:close( Socket )
    end.

lst_binary_to_integer( L ) -> [ list_to_integer( binary_to_list( B ) ) || B <- L ].
% <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n

pre_store( Bin1 , S  , HasCas) ->
    Len = byte_size( Bin1 ) -2
    ,<<Bin:Len/binary , "\r\n">> = Bin1
    ,[Key,Flags1,Exptime1,Bytes1|ElseCmd] = lists:filter(fun(<<>>) -> false ; (_) ->  true end ,binary:split(Bin,<<" ">> , [global]) ) 
    ,[Flags ,Exptime , Bytes ] = lst_binary_to_integer( [Flags1 , Exptime1 , Bytes1] )
%when ElseCmd =:= [] orelse ElseCmd =:= [<<"noreply">>] 
    ,{ ElseCmd1 , Reply } = case length( ElseCmd ) of
        0 -> { [] , reply }
        ;_ -> 
            case lists:last( ElseCmd ) of
                <<"noreply">> -> 
                    { lists:delete( <<"noreply">>,  ElseCmd ) , noreply }
                ;_ -> {ElseCmd , reply}
            end
    end
    ,Cas = case ElseCmd1 of
        [ OldVer1 ] -> 
            [OldVer] = lst_binary_to_integer( [OldVer1] )
            ,OldVer
        ;[] -> no
        ;_ -> throw( error )
    end
    ,Expr = if 
        Exptime =:= 0 -> 0
        ;Exptime > 2592000 -> Exptime %60 * 60 * 24 * 30
        ;true -> bnow_timer:now() + Exptime
    end
    ,if 
        HasCas =:= no_cas andalso Cas =:= no ->
            case wait_data( S , Bytes ) of
                {ok , Content} ->
                    {Reply, { { Key , Flags , Expr , Bytes } , Content }}
            end
        ;HasCas =/= no_cas andalso Cas =/= no ->
            case wait_data( S , Bytes ) of
                {ok , Content} ->
                    {Reply, { { Key , Flags , Expr , Bytes , Cas } , Content }}
            end
        ;true -> error
    end
    .
store( Bin,S , Fun ) ->
    store(Bin, S , Fun , no_cas).
store( Bin,S , Fun , HasCas ) ->
    case pre_store( Bin,S , HasCas ) of
        {Act,Fields} ->
            case Fun(Fields) of
                R when is_record( R , ememcached ) ->
                    Rs = write( R )
                    ,if
                        Act =:= reply ->
                            case Rs of
                                ok -> gen_tcp:send( S , <<"STORED\r\n">> )
                                ;_ -> gen_tcp:send(S , <<"NOT_STORED\r\n">>)
                            end
                        ;true -> ok
                    end
                ;El -> throw(El)
            end
        ;Else -> throw(Else)
    end.

write( F ) ->
    mnesia:dirty_write( F#ememcached{ ver = mnesia:dirty_update_counter( ememcached_ver , memcached_ver , 1 )} ).

regular( Bin1 , Act , S ) ->
    Len = byte_size( Bin1 ) -2
    ,<<Bin:Len/binary , "\r\n">> = Bin1
    ,Now = bnow_timer:now()
    ,case lists:filter( fun(<<>>) -> false ; (_) -> true end , binary:split(Bin , <<" ">> , [global]) ) of
        [Key , Step | Else] -> 
            if
                Else =:= [] orelse Else =:= [<<"noreply">>] ->
                    case mnesia:dirty_read( ememcached , Key ) of
                        [ M ] when M#ememcached.expr =:= 0 orelse M#ememcached.expr > Now -> 
                            try 
                                Step1 = list_to_integer( binary_to_list( Step ) )
                                ,try
                                    Value = if
                                        Act =:= <<"incr">> -> 
                                            list_to_integer( binary_to_list( M#ememcached.value ) ) + Step1
                                        ;true -> 
                                            list_to_integer( binary_to_list( M#ememcached.value ) ) - Step1 
                                    end
                                    ,Value1 = if 
                                        Value < 0 -> <<"0">>
                                        ;true -> list_to_binary(integer_to_list(Value)) 
                                    end 
                                    ,write( M#ememcached{ value= Value1 } )
                                    ,if
                                        Else =:= [] -> gen_tcp:send(S , <<Value1/binary, "\r\n">>)
                                        ;true -> ok
                                    end
                                catch
                                    _:_ -> gen_tcp:send( S , <<"CLIENT_ERROR cannot ",Act/binary,"ement or decrement non-numeric value\r\n">> )
                                end
                            catch
                                _:_ -> gen_tcp:send( S , <<"CLIENT_ERROR invalid numeric delta argument\r\n">> )
                            end
                        ;_ ->
                            gen_tcp:send( S , <<"NOT_FOUND\r\n">> )
                    end
                ;true -> 
                    gen_tcp:send( S , <<"ERROR\r\n">> )
            end
    end.


wait_data(S , Len) ->
    receive 
        {tcp , S , Text} -> 
            L = byte_size(Text)
            ,if 
                L - 2 =:= Len -> <<Msg:Len/binary,_/binary>> = Text ,  {ok , Msg}
                ;true -> error
            end
    end.

get(Bin , S,Act) ->
    Len = byte_size( Bin ) -2
    ,<<Keys:Len/binary , "\r\n">> = Bin
    ,Now = bnow_timer:now()
    ,F = fun(Y)  -> 
        case ets:lookup( ememcached , Y ) of
            [V|_] when V#ememcached.expr >= Now orelse V#ememcached.expr =:= 0  ->
                gen_tcp:send(S , [<<"VALUE ">> , V#ememcached.key ,<<" ">> , integer_to_list(V#ememcached.flag) ,<<" ">> , integer_to_list( byte_size(V#ememcached.value) ) 
                ,if 
                    Act =:= get -> <<>>
                    ;true -> [<<" ">> , integer_to_list( V#ememcached.ver ) ]
                end
                , <<"\r\n">>])
                ,gen_tcp:send(S , <<(V#ememcached.value)/binary , "\r\n">>)
            ;_ -> ok
        end
    end
    ,[ F(X) || X <- lists:filter(fun(<<>>) -> false ; (_) ->  true end ,binary:split( Keys , <<" ">> ,[global]) ) ]
    ,gen_tcp:send( S, <<"END\r\n">> ).

delete( Key ) -> 
    case mnesia:dirty_read( ememcached , Key ) of
        [] -> false
        ;[D] -> 
            mnesia:dirty_delete( {ememcached , Key} )
            ,Now = bnow_timer:now()
            ,if
                D#ememcached.expr > 0 andalso D#ememcached.expr =< Now -> false
                ;true -> true
            end
    end.

touch( Key , Expr ) ->
    Exptime1 = list_to_integer( binary_to_list(Expr) )
    ,Now = bnow_timer:now()
    ,Exptime = if
        Exptime1 =:= 0 -> 0
        ;Exptime1 > 2592000 -> Exptime1 %60 * 60 * 24 * 30
        ;true ->  Now + Exptime1
    end
    ,case mnesia:dirty_read( ememcached , Key ) of
        [ D ] when D#ememcached.expr =:= 0 orelse D#ememcached.expr > Now -> 
            mnesia:dirty_write( D#ememcached{ expr = Exptime } )
            ,ok
        ;_ -> not_found
    end.

%%
%% store api
%% <command name> is "set", "add", "replace", "append" or "prepend"
%%
do(<<"set ", Bin/binary>> , S) ->
    F = fun({ { Key , Flags , Exptime , _Bytes } , Content }) -> 
        #ememcached{ key = Key , value = Content , flag = Flags, expr = Exptime }
    end
    ,store( Bin , S , F )
;do(<<"add ", Bin/binary>> , S) ->
    F = fun({ { Key , Flags , Exptime , _Bytes } , Content }) -> 
        case mnesia:dirty_read( ememcached , Key ) of
            [Mem] -> 
                Now = bnow_timer:now()
                ,if
                    Mem#ememcached.expr > 0 andalso Mem#ememcached.expr < Now -> 
                    #ememcached{ key = Key , value = Content , flag = Flags , expr = Exptime }
                    ;true -> not_stored
                end
            ;[] -> 
                #ememcached{ key = Key , value = Content ,flag = Flags, expr = Exptime }
        end
    end
    ,store( Bin , S , F )
;do(<<"replace ", Bin/binary>> , S) ->
    F = fun({ { Key , Flags , Exptime , _Bytes } , Content }) -> 
        case mnesia:dirty_read( ememcached , Key ) of
            [Mem] -> 
                Now = bnow_timer:now()
                ,if
                    Mem#ememcached.expr > 0 andalso Mem#ememcached.expr < Now -> error
                    ;true -> #ememcached{ key = Key , value = Content , flag = Flags, expr = Exptime }
                end
            ;[] -> not_stored
        end
    end
    ,store( Bin , S , F )
;do(<<"append ", Bin/binary>>,S) ->
    F = fun({ { Key , Flags , Exptime , _Bytes } , Content }) -> 
        case mnesia:dirty_read( ememcached , Key ) of
            [Mem] -> 
                Now = bnow_timer:now()
                ,if
                    Mem#ememcached.expr > 0 andalso Mem#ememcached.expr < Now -> error
                    ;true -> #ememcached{ key = Key , value = <<(Mem#ememcached.value)/binary , Content/binary>> , flag = Flags, expr = Exptime }
                end
            ;[] -> not_stored
        end
    end
    ,store( Bin , S , F )
;do(<<"prepend ", Bin/binary>>,S) ->
    F = fun({ { Key , Flags , Exptime , _Bytes } , Content }) -> 
        case mnesia:dirty_read( ememcached , Key ) of
            [Mem] -> 
                Now = bnow_timer:now()
                ,if
                    Mem#ememcached.expr > 0 andalso Mem#ememcached.expr < Now -> error
                    ;true -> #ememcached{ key = Key , value = <<Content/binary, (Mem#ememcached.value)/binary >> , flag = Flags, expr = Exptime }
                end
            ;[] -> not_stored
        end
    end
    ,store( Bin , S , F )
;do(<<"cas " , Bin/binary>> , S ) ->
    F = fun({ { Key , Flags , Expr , _Bytes , Cas } , Content }) ->
        case mnesia:dirty_read( ememcached , Key ) of
            [Mem] -> 
                Now = bnow_timer:now()
                ,if
                    Mem#ememcached.expr > 0 andalso Mem#ememcached.expr < Now -> error
                    ;true -> 
                        if 
                            Mem#ememcached.ver =:= Cas ->
                                #ememcached{ 
                                    key = Key 
                                    , value = Content 
                                    , flag = Flags
                                    , expr = Expr
                                }
                            ;true -> 
                                exists
                        end
                end
            ;[] -> not_found
        end
    end
    ,store( Bin , S ,F, cas )
    
;do(<<"incr ", Bin1/binary>> , S ) ->
    regular( Bin1 , <<"incr">> , S )
;do(<<"decr ", Bin1/binary>> , S ) ->
    regular( Bin1 , <<"decr">> , S )

;do(<<"touch " , Bin1/binary>> , S) ->
    Len = byte_size( Bin1 ) -2
    ,<<Bin:Len/binary , "\r\n">> = Bin1
    ,case lists:filter(fun(<<>>) -> false ; (_) ->  true end ,binary:split( Bin , <<" ">> , [global] ) ) of
        [ Key , Expr | Else ] ->
            case { touch( Key , Expr ) , Else } of
                { ok ,[] } -> gen_tcp:send(S , <<"TOUCHED\r\n">>)
                ;{ not_found ,[] } -> gen_tcp:send(S , <<"NOT_FOUND\r\n">>)
                ;{_ , [<<"noreply">>]} -> ok
            end
    end

;do(<<"get ",Bin/binary>>,S) ->
    get( Bin , S , get )
;do(<<"gets ",Bin/binary>>,S) ->
    get( Bin , S , gets )
    
;do(<<"delete ",Bin1/binary>>,S) ->
    Len = byte_size( Bin1 ) -2
    ,<<Bin:Len/binary , "\r\n">> = Bin1
    ,case lists:filter(fun(<<>>) -> false ; (_) ->  true end ,binary:split( Bin , <<" ">> , [global] ) ) of
        [ Key |Noreply ] -> 
            case { delete( Key ) , Noreply }  of
                {true , [] } -> gen_tcp:send( S , <<"DELETED\r\n">> )
                ;{false , [] } -> gen_tcp:send( S , <<"NOT_FOUND\r\n">> )
                ;{_ , [<<"noreply">>]} -> ok
                ;_ -> gen_tcp:send(S , <<"CLIENT_ERROR bad command line format.  Usage: delete <key> [noreply]\r\n">>)
            end
        ;_ -> gen_tcp:send(S , <<"CLIENT_ERROR bad command line format.  Usage: delete <key> [noreply]\r\n">>)
    end

;do(<<"flush_all" , _/binary>>,S) ->
    mnesia:clear_table( ememcached ),
    gen_tcp:send(S , <<"OK\r\n">>)

;do(_,S) ->
    gen_tcp:send(S , <<"ERROR\r\n">>).
    %"CLIENT_ERROR Msg\r\n".

