-module(antidote_crdt_generic).

%% Callbacks
-export([downstream/2, equal/2, from_binary/1,
	 is_operation/1, new/0, require_state_downstream/1,
	 to_binary/1, update/2, value/1]).

-export_type([antidote_crdt_generic/0,
	      binary_antidote_crdt_generic/0,
	      antidote_crdt_generic_op/0]).

-behaviour(antidote_crdt).

-define(HOSTFILE, "host_config.txt").

gethost(FILE) ->
    % [read] is a list of modes that the file should be opened as
    {ok, Device} = file:open(FILE, [read]),
    try io:get_line(Device, "") after
      file:close(Device)
    end.

-define(BACKEND, list_to_atom(gethost(?HOSTFILE))).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-endif.

-opaque antidote_crdt_generic() :: objectid().

-type binary_antidote_crdt_generic() :: binary().

-type antidote_crdt_generic_op() :: {invoke, data()} |
				    {invoke_all, [data()]}.

-type downstream_op() :: [data()].

-type objectid() :: binary().

%%-type data() :: {binary(), non_neg_integer()}.
-type data() :: {binary()}.

-spec new() -> antidote_crdt_generic().

new() -> unique().

-spec value(antidote_crdt_generic()) -> objectid().

value(Generic) ->
    net_kernel:connect_node(?BACKEND), % creates association if not already there
    {javamailbox, ?BACKEND} !
      {self(), {Generic, read}}, % sends the read call
    R = receive
	  error -> throw("Oh no, an error has occurred");
	  M -> M
	  after 5000 -> io:fwrite("No answer~n"), {"no answer!"}
	end,
    R.

-spec downstream(antidote_crdt_generic_op(),
		 antidote_crdt_generic()) -> {ok, downstream_op()}.

downstream({invoke, Elem}, Generic) ->
    downstream({invoke_all, [Elem]}, Generic);
%% rethink this
downstream({invoke_all, Elems}, _Generic) ->
    {ok, Elems}.

unique() -> crypto:strong_rand_bytes(20).

-spec update(downstream_op(),
	     antidote_crdt_generic()) -> {ok,
					  antidote_crdt_generic()}.

last_elm([]) -> [];
last_elm([A]) -> A;
last_elm([_ | A]) -> last_elm(A);
last_elm(A) -> A.

% Want only the last genericid in list, maybe just use the Generic variable instead?
update(DownstreamOp, Generic) ->
    {ok,
     last_elm(apply_downstreams(DownstreamOp, Generic))}.

apply_downstreams([], Generic) -> Generic;
apply_downstreams([Binary1 | OpsRest], Generic) ->
    [apply_downstream(Binary1, Generic)
     | apply_downstreams(OpsRest, Generic)].

apply_downstream(Binary, Generic) ->
    % send and recieve message
    net_kernel:connect_node(?BACKEND), % creates association if not already there
    {javamailbox, ?BACKEND} !
      {self(),
       {Generic, invoke, Binary}}, % sends the generic function
    R = receive
	  error -> throw("Oh no, an error has occurred");
	  _M -> {Generic}
	  after 5000 -> {"no answer!"}
	end,
    R.

snapshot(DownstreamOp, Generic) ->
	{ok, _} = update(DownstreamOp, Generic),
	net_kernel:connect_node(?BACKEND), % creates association if not already there
    {javamailbox, ?BACKEND} !
      {self(),
       {Generic, snapshot}}, % sends the generic function
    R = receive
	  error -> throw("Oh no, an error has occurred");
	  M -> M
	  after 5000 -> {"no answer!"}
	end,
    {ok, R}.


%% Later work in a better equality
-spec equal(antidote_crdt_generic(),
	    antidote_crdt_generic()) -> boolean().

equal(GenericA, GenericB) -> GenericA == GenericB.

-spec
     to_binary(antidote_crdt_generic()) -> binary_antidote_crdt_generic().

to_binary(Binary) -> Binary.

from_binary(<<Bin/binary>>) -> {ok, Bin}.

is_operation({invoke, _Elem}) -> true;
is_operation({invoke_all, L}) when is_list(L) -> true;
is_operation(_) -> false.

require_state_downstream(_) -> false.

%% TESTS
-ifdef(TEST).

prepare_and_effect(Op, Generic) ->
    {ok, Downstream} = downstream(Op, Generic),
    update(Downstream, Generic).

update_invoke_test() ->
    net_kernel:start([erlnode, shortnames]),
    Counter_object = <<172, 237, 0, 5, 115, 114, 0, 12, 109,
		       97, 105, 110, 46, 67, 111, 117, 110, 116, 101, 114, 0,
		       0, 0, 0, 0, 0, 0, 1, 2, 0, 1, 73, 0, 5, 99, 111, 117,
		       110, 116, 120, 112, 0, 0, 0, 0>>,
    Increment_1_object = <<172, 237, 0, 5, 115, 114, 0, 20,
			   109, 97, 105, 110, 46, 71, 101, 110, 101, 114, 105,
			   99, 70, 117, 110, 99, 116, 105, 111, 110, 0, 0, 0, 0,
			   0, 0, 0, 1, 2, 0, 2, 76, 0, 8, 65, 114, 103, 117,
			   109, 101, 110, 116, 116, 0, 18, 76, 106, 97, 118, 97,
			   47, 108, 97, 110, 103, 47, 79, 98, 106, 101, 99, 116,
			   59, 76, 0, 12, 70, 117, 110, 99, 116, 105, 111, 110,
			   78, 97, 109, 101, 116, 0, 18, 76, 106, 97, 118, 97,
			   47, 108, 97, 110, 103, 47, 83, 116, 114, 105, 110,
			   103, 59, 120, 112, 115, 114, 0, 17, 106, 97, 118, 97,
			   46, 108, 97, 110, 103, 46, 73, 110, 116, 101, 103,
			   101, 114, 18, 226, 160, 164, 247, 129, 135, 56, 2, 0,
			   1, 73, 0, 5, 118, 97, 108, 117, 101, 120, 114, 0, 16,
			   106, 97, 118, 97, 46, 108, 97, 110, 103, 46, 78, 117,
			   109, 98, 101, 114, 134, 172, 149, 29, 11, 148, 224,
			   139, 2, 0, 0, 120, 112, 0, 0, 0, 1, 116, 0, 9, 105,
			   110, 99, 114, 101, 109, 101, 110, 116>>,
    Decrement_1_object = <<172, 237, 0, 5, 115, 114, 0, 20,
			   109, 97, 105, 110, 46, 71, 101, 110, 101, 114, 105,
			   99, 70, 117, 110, 99, 116, 105, 111, 110, 0, 0, 0, 0,
			   0, 0, 0, 1, 2, 0, 2, 76, 0, 8, 65, 114, 103, 117,
			   109, 101, 110, 116, 116, 0, 18, 76, 106, 97, 118, 97,
			   47, 108, 97, 110, 103, 47, 79, 98, 106, 101, 99, 116,
			   59, 76, 0, 12, 70, 117, 110, 99, 116, 105, 111, 110,
			   78, 97, 109, 101, 116, 0, 18, 76, 106, 97, 118, 97,
			   47, 108, 97, 110, 103, 47, 83, 116, 114, 105, 110,
			   103, 59, 120, 112, 115, 114, 0, 17, 106, 97, 118, 97,
			   46, 108, 97, 110, 103, 46, 73, 110, 116, 101, 103,
			   101, 114, 18, 226, 160, 164, 247, 129, 135, 56, 2, 0,
			   1, 73, 0, 5, 118, 97, 108, 117, 101, 120, 114, 0, 16,
			   106, 97, 118, 97, 46, 108, 97, 110, 103, 46, 78, 117,
			   109, 98, 101, 114, 134, 172, 149, 29, 11, 148, 224,
			   139, 2, 0, 0, 120, 112, 0, 0, 0, 1, 116, 0, 9, 100,
			   101, 99, 114, 101, 109, 101, 110, 116>>,
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
    ?assertEqual(Counter_value_0, (value(Generic3))).

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
