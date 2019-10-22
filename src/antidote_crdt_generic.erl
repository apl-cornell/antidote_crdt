-module(antidote_crdt_generic).

%% Callbacks
-export([ new/0,
          value/1,
          downstream/2,
          update/2,
          equal/2,
          to_binary/1,
          from_binary/1,
          is_operation/1,
          require_state_downstream/1
        ]).

-behaviour(antidote_crdt).

-export_type([antidote_crdt_generic/0, binary_antidote_crdt_generic/0, antidote_crdt_generic_op/0]).

-opaque antidote_crdt_generic() :: objectid().

-type binary_antidote_crdt_generic() :: binary().
%% A binary that from_binary/1 will operate on.

-type antidote_crdt_generic_op() ::
      {invoke, data()}
    | {invoke_all, [data()]}.

%% element, AddTime, RemoveTime
-type downstream_op() :: [data()].

-type objectid() :: binary().
-type data() :: {binary(), non_neg_integer()}.

-spec new() -> antidote_crdt_generic().
new() -> unique().

%% add a value call to server
% will need to set up something on the java side for reads
-spec value(antidote_crdt_generic()) -> [term()].
value(Generic) -> Generic.

-spec downstream(antidote_crdt_generic_op(), antidote_crdt_generic()) -> {ok, downstream_op()}.
downstream({invoke, Elem}, Generic) ->
  downstream({invoke_all, [Elem]}, Generic);
downstream({invoke_all, Elems}, Generic) ->
  CreateDownstream = fun(Elem, Id) ->
      {Elem, Id}
  end,
  DownstreamOps = create_downstreams(CreateDownstream, lists:usort(Elems), Generic, []),
  {ok, lists:reverse(DownstreamOps)}.

create_downstreams(_CreateDownstream, [], _Generic, DownstreamOps) ->
  DownstreamOps;
create_downstreams(_CreateDownstream, _Elems, [], DownstreamOps) ->
  DownstreamOps;
%% will need to do time ordering
create_downstreams(CreateDownstream, [Elem1|ElemsRest], Generic, DownstreamOps) ->
  DownstreamOp = CreateDownstream(Elem1, Generic),
  create_downstreams(CreateDownstream, ElemsRest, Generic, [DownstreamOp|DownstreamOps]).

%% @doc generate a unique identifier (best-effort).
unique() ->
    crypto:strong_rand_bytes(20).

-spec update(downstream_op(), antidote_crdt_generic()) -> {ok, antidote_crdt_generic()}.
  update(DownstreamOp, Generic) ->
    {ok, apply_downstreams(DownstreamOp, Generic)}.

%% @private apply a list of downstream ops to a given orset
apply_downstreams([], Generic) -> Generic;
%% When I do time ordering in create_downstream, will not longer need time here
apply_downstreams([{Binary1, _Time}|OpsRest], Generic) ->
      [apply_downstream(Binary1, Generic) | apply_downstreams(OpsRest, Generic)].

%% @private create an orddict entry from a downstream op
apply_downstream(Binary, Generic) ->
  % send and recieve message
  %% need to have set up node on initialization with % erl -name JavaNode
  net_adm:ping(foo@domain), % creates association if not already there
  PID = spawn(?MODULE, java-server, []),
  PID ! {Generic, Binary}, % sends message
  R = receive
      _M -> {Generic}
  after 5000 -> {error,"no answer!"}
  end,
  {R}.

%% Later work in a better equality
-spec equal(antidote_crdt_generic(), antidote_crdt_generic()) -> boolean().
equal(GenericA, GenericB) ->
    GenericA == GenericB.

-spec to_binary(antidote_crdt_generic()) -> binary_antidote_crdt_generic().
to_binary(Binary) -> Binary.

from_binary(<<Bin/binary>>) -> {ok, Bin}.

is_operation({invoke, _Elem}) -> true;
is_operation({invoke_all, L}) when is_list(L) -> true;
is_operation(_) -> false.

require_state_downstream(_) -> true.