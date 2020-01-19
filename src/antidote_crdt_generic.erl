-module(antidote_crdt_generic).

%% Callbacks
-export([downstream/2, equal/2, from_binary/1,
	 is_operation/1, new/0, require_state_downstream/1,
	 snapshot/2, to_binary/1, update/2, value/1]).

-export_type([antidote_crdt_generic/0,
	      binary_antidote_crdt_generic/0,
	      antidote_crdt_generic_op/0]).

-behaviour(antidote_crdt).

gethost() ->
    % First see if we have the node name declared at the antidote level(vm.args.src). If not grab node name from antidote_crdt.app.src...this is usually for testing
    case os:getenv("backend_node") of
      false ->
	  application:load(antidote_crdt),
	  {ok, Node} = application:get_env(antidote_crdt,
					   backend_node),
	  Node;
      Node -> Node
    end.

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-endif.

-opaque antidote_crdt_generic() :: objectid().

-type binary_antidote_crdt_generic() :: binary().

-type antidote_crdt_generic_op() :: {invoke, data()} |
				    {invoke_all, [data()]}.

-type downstream_op() :: [data()].

-type objectid() :: {binary(), binary()}.

-type data() :: {binary()}.

-spec new() -> antidote_crdt_generic().

new() -> {unique(), <<>>}.

-spec value(antidote_crdt_generic()) -> binary().

value({JavaId, JavaObject}) ->
    net_kernel:connect_node(gethost()), % creates association if not already there
    {javamailbox, gethost()} !
      {self(), {JavaId, read}}, % sends the read call
    R = receive
	  error -> throw("Oh no, an error has occurred");
	  getobject ->
	      io:fwrite("There has been a request for the object"),
	      {javamailbox, gethost()} !
		{self(), {JavaId, invoke, JavaObject}},
	      receive
		error ->
		    io:fwrite("Something happened while we were trying "
			      "to update the JavaObject"),
		    throw("Oh no, an error has occurred");
		_M -> value({JavaId, JavaObject})
		after 5000 -> io:fwrite("No answer~n"), {"no answer!"}
	      end;
	  M -> M
	  after 5000 -> io:fwrite("No answer~n"), {"no answer!"}
	end,
    R.

-spec downstream(antidote_crdt_generic_op(),
		 antidote_crdt_generic()) -> {ok, downstream_op()}.

downstream({invoke, Elem}, Generic) ->
    downstream({invoke_all, [Elem]}, Generic);
downstream({invoke_all, Elems}, _Generic) ->
    {ok, Elems}.

unique() -> crypto:strong_rand_bytes(20).

last_elm([]) -> [];
last_elm([A]) -> A;
last_elm([_ | A]) -> last_elm(A);
last_elm(A) -> A.

-spec update(downstream_op(),
	     antidote_crdt_generic()) -> {ok,
					  antidote_crdt_generic()}.

% Want only the last genericid in list, maybe just use the Generic variable instead?
update(DownstreamOp, Generic) ->
    io:fwrite("Got update~n"),
    {ok,
     last_elm(apply_downstreams(DownstreamOp, Generic))}.

apply_downstreams([], Generic) -> Generic;
apply_downstreams([Binary1 | OpsRest], Generic) ->
    io:fwrite("Splitting downstream~n"),
    [apply_downstream(Binary1, Generic)
     | apply_downstreams(OpsRest, Generic)].

apply_downstream(Binary,
		 {JavaId, JavaObject} = Generic) ->
    % send and recieve message
    io:fwrite("Start apply~n"),
    io:fwrite(gethost()),
    io:fwrite("~n"),
    true =
	net_kernel:connect_node(gethost()), % creates association if not already there
    io:fwrite("Connected~n"),
    {javamailbox, 'JavaNode@127.0.0.1'} !
      {self(),
       {JavaId, invoke, Binary}}, % sends the generic function
    R = receive
	  error ->
	      io:fwrite("Oh no, there has been an error with "
			"the backend~n"),
	      throw("Oh no, an error has occurred");
	  getobject ->
	      io:fwrite("There has been a request for the object"),
	      {javamailbox, gethost()} !
		{self(), {JavaId, invoke, JavaObject}},
	      receive
		error ->
		    io:fwrite("Something happened while we were trying "
			      "to update the JavaObject"),
		    throw("Oh no, an error has occurred");
		_M -> apply_downstream(Binary, Generic)
		after 5000 -> io:fwrite("No answer~n"), {"no answer!"}
	      end;
	  _M -> {Generic}
	  after 5000 -> io:fwrite("No answer~n"), {"no answer!"}
	end,
    R.

snapshot(DownstreamOp,
	 {JavaId, _JavaObject} = Generic) ->
    % send and recieve message
    {ok, _} = update(DownstreamOp, Generic),
    net_kernel:connect_node(gethost()), % creates association if not already there
    {javamailbox, gethost()} !
      {self(),
       {JavaId, snapshot}}, % sends the generic function
    R = receive
	  error -> throw("Oh no, an error has occurred");
	  M -> M
	  after 5000 -> {"no answer!"}
	end,
    {ok, R}.

-spec equal(antidote_crdt_generic(),
	    antidote_crdt_generic()) -> boolean().

equal(GenericA, GenericB) -> GenericA == GenericB.

%%equal(_GenericA, _GenericB) ->
%%    throw("Waiiiittt... we actually use equality?? "
%%	  "Please review antidote_crdt_generic "
%%	  "in the antidote_crdt repo").

-spec
     to_binary(antidote_crdt_generic()) -> binary_antidote_crdt_generic().

%%to_binary(Binary) -> Binary.
to_binary({JavaId, JavaObject}) ->
    <<JavaId:20/binary, JavaObject/binary>>.

from_binary(<<JavaId:20/binary, JavaObject/binary>>) ->
    {ok, {JavaId, JavaObject}}.

is_operation({invoke, _Elem}) -> true;
is_operation({invoke_all, L}) when is_list(L) -> true;
is_operation(_) -> false.

%% I ignore the `Generic` state in downstream so this is not needed
require_state_downstream(_) -> false.

%% TESTS
-ifdef(TEST).

prepare_and_effect(Op, Generic) ->
    {ok, Downstream} = downstream(Op, Generic),
    update(Downstream, Generic).

update_invoke_test() ->
    net_kernel:start([erlnode, longnames]),
    erlang:set_cookie(node(), antidote),
    % may be outdated
    Counter_object = <<172, 237, 0, 5, 115, 114, 0, 12, 109,
		       97, 105, 110, 46, 67, 111, 117, 110, 116, 101, 114, 0,
		       0, 0, 0, 0, 0, 0, 1, 2, 0, 2, 73, 0, 5, 99, 111, 117,
		       110, 116, 76, 0, 5, 73, 100, 83, 101, 116, 116, 0, 15,
		       76, 106, 97, 118, 97, 47, 117, 116, 105, 108, 47, 83,
		       101, 116, 59, 120, 112, 0, 0, 0, 0, 115, 114, 0, 17,
		       106, 97, 118, 97, 46, 117, 116, 105, 108, 46, 72, 97,
		       115, 104, 83, 101, 116, 186, 68, 133, 149, 150, 184,
		       183, 52, 3, 0, 0, 120, 112, 119, 12, 0, 0, 0, 16, 63,
		       64, 0, 0, 0, 0, 0, 0, 120>>,
    Increment_1_object = <<172, 237, 0, 5, 115, 114, 0, 20,
			   109, 97, 105, 110, 46, 71, 101, 110, 101, 114, 105,
			   99, 70, 117, 110, 99, 116, 105, 111, 110, 0, 0, 0, 0,
			   0, 0, 0, 1, 2, 0, 3, 76, 0, 8, 65, 114, 103, 117,
			   109, 101, 110, 116, 116, 0, 18, 76, 106, 97, 118, 97,
			   47, 108, 97, 110, 103, 47, 79, 98, 106, 101, 99, 116,
			   59, 76, 0, 12, 70, 117, 110, 99, 116, 105, 111, 110,
			   78, 97, 109, 101, 116, 0, 18, 76, 106, 97, 118, 97,
			   47, 108, 97, 110, 103, 47, 83, 116, 114, 105, 110,
			   103, 59, 76, 0, 2, 73, 100, 116, 0, 19, 76, 106, 97,
			   118, 97, 47, 108, 97, 110, 103, 47, 73, 110, 116,
			   101, 103, 101, 114, 59, 120, 112, 115, 114, 0, 17,
			   106, 97, 118, 97, 46, 108, 97, 110, 103, 46, 73, 110,
			   116, 101, 103, 101, 114, 18, 226, 160, 164, 247, 129,
			   135, 56, 2, 0, 1, 73, 0, 5, 118, 97, 108, 117, 101,
			   120, 114, 0, 16, 106, 97, 118, 97, 46, 108, 97, 110,
			   103, 46, 78, 117, 109, 98, 101, 114, 134, 172, 149,
			   29, 11, 148, 224, 139, 2, 0, 0, 120, 112, 0, 0, 0, 1,
			   116, 0, 9, 105, 110, 99, 114, 101, 109, 101, 110,
			   116, 115, 113, 0, 126, 0, 5, 0, 52, 132, 82>>,
    Decrement_1_object = <<172, 237, 0, 5, 115, 114, 0, 20,
			   109, 97, 105, 110, 46, 71, 101, 110, 101, 114, 105,
			   99, 70, 117, 110, 99, 116, 105, 111, 110, 0, 0, 0, 0,
			   0, 0, 0, 1, 2, 0, 3, 76, 0, 8, 65, 114, 103, 117,
			   109, 101, 110, 116, 116, 0, 18, 76, 106, 97, 118, 97,
			   47, 108, 97, 110, 103, 47, 79, 98, 106, 101, 99, 116,
			   59, 76, 0, 12, 70, 117, 110, 99, 116, 105, 111, 110,
			   78, 97, 109, 101, 116, 0, 18, 76, 106, 97, 118, 97,
			   47, 108, 97, 110, 103, 47, 83, 116, 114, 105, 110,
			   103, 59, 76, 0, 2, 73, 100, 116, 0, 19, 76, 106, 97,
			   118, 97, 47, 108, 97, 110, 103, 47, 73, 110, 116,
			   101, 103, 101, 114, 59, 120, 112, 115, 114, 0, 17,
			   106, 97, 118, 97, 46, 108, 97, 110, 103, 46, 73, 110,
			   116, 101, 103, 101, 114, 18, 226, 160, 164, 247, 129,
			   135, 56, 2, 0, 1, 73, 0, 5, 118, 97, 108, 117, 101,
			   120, 114, 0, 16, 106, 97, 118, 97, 46, 108, 97, 110,
			   103, 46, 78, 117, 109, 98, 101, 114, 134, 172, 149,
			   29, 11, 148, 224, 139, 2, 0, 0, 120, 112, 0, 0, 0, 1,
			   116, 0, 9, 100, 101, 99, 114, 101, 109, 101, 110,
			   116, 115, 113, 0, 126, 0, 5, 0, 75, 118, 202>>,
    Counter_value_0 = <<172, 237, 0, 5, 115, 114, 0, 17,
			106, 97, 118, 97, 46, 108, 97, 110, 103, 46, 73, 110,
			116, 101, 103, 101, 114, 18, 226, 160, 164, 247, 129,
			135, 56, 2, 0, 1, 73, 0, 5, 118, 97, 108, 117, 101, 120,
			114, 0, 16, 106, 97, 118, 97, 46, 108, 97, 110, 103, 46,
			78, 117, 109, 98, 101, 114, 134, 172, 149, 29, 11, 148,
			224, 139, 2, 0, 0, 120, 112, 0, 0, 0, 0>>,
    Counter_value_1 = <<172, 237, 0, 5, 115, 114, 0, 17,
			106, 97, 118, 97, 46, 108, 97, 110, 103, 46, 73, 110,
			116, 101, 103, 101, 114, 18, 226, 160, 164, 247, 129,
			135, 56, 2, 0, 1, 73, 0, 5, 118, 97, 108, 117, 101, 120,
			114, 0, 16, 106, 97, 118, 97, 46, 108, 97, 110, 103, 46,
			78, 117, 109, 98, 101, 114, 134, 172, 149, 29, 11, 148,
			224, 139, 2, 0, 0, 120, 112, 0, 0, 0, 1>>,
    Generic = new(),
    ?assertEqual(Counter_value_0,
		 (value({unique(), Counter_object}))),
    {ok, Generic1} = prepare_and_effect({invoke,
					 Counter_object},
					Generic),
    ?assertEqual(Counter_value_0, (value(Generic1))),
    {ok, Generic2} = prepare_and_effect({invoke,
					 Increment_1_object},
					Generic1),
    ?assertEqual(Counter_value_1, (value(Generic2))),
    {ok, Generic3} = prepare_and_effect({invoke,
					 Decrement_1_object},
					Generic2),
    ?assertEqual(Counter_value_0,
		 (value(Generic3))).    %% snapshot check

equal_test() ->
    Generic1 = new(),
    Generic2 = new(),
    ?assertNot((equal(Generic1, Generic2))),
    ?assert((equal(Generic1, Generic1))).

binary_test() ->
    Generic = new(),
    B = to_binary(Generic),
    ?assertEqual({ok, Generic}, (from_binary(B))).

is_operation_test() ->
    ?assertEqual(true, (is_operation({invoke, {<<0>>}}))),
    %?assertEqual(true,
    %(is_operation({invoke_all, {[<<0>>]}}))),
    ?assertEqual(false,
		 (is_operation({anything, [1, 2, 3]}))).

-endif.
