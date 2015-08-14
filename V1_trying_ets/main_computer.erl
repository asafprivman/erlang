-module(main_computer).
-export([test/0]).

%%%%%%%%%%%%% Main: %%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%% Test Bench %%%%%
test() ->
    NumOfNodes = 3,
    NumOfLayers = 5,
    RootArticle = 0,
    create_test_database(),
    create_nodes_for_test(NumOfNodes),
    Tree = init_main(NumOfNodes, NumOfLayers, RootArticle),
    io:fwrite("Tree:[~p].~n",[Tree]),
    io:fwrite("Note to self, check tree correct add an assert ExpectedTree = Tree.~n",[]).

create_nodes_for_test(NumOfNodes) ->
    [spawn(main_computer, init_node, [NodeID, self()]) || NodeID<-NumOfNodes].

%%%% Tested code %%%%%
init_main(NumOfNodes, NumOfLayers, RootArticle) ->
    send_ets_to_nodes(NumOfNodes),
    Tree = create_tree(RootArticle, NumOfLayers),
    kill_mappers(),
    Tree.


%TODO:send ets
send_ets_to_nodes(NumOfNodes) ->
    Mappers = receive_mappers([], NumOfNodes),
    put(mapper_pids, Mappers),
    % TODO:send ets
    send_mappers_all_mappers(Mappers),
    put(mappers, Mappers).

receive_mappers(Mappers, 0) ->
    Mappers;
receive_mappers(Mappers, NumOfNodesLeft) ->
    receive
	{mappers_created, NodeNum, MapperPids} ->
	    receive_mappers([MapperPids|Mappers], NumOfNodesLeft-1)
    end.

send_mappers_all_mappers(Mappers) ->
    [Mapper ! {start, self(), Mappers} || Mapper <- Mappers].

%%%%%%% create tree %%%%%%% 
create_tree(RootArticle, NumOfLayers) ->
    Ref = erlang:make_ref(),
    send_mapper(root, RootArticle, NumOfLayers, Ref),
    Tree = start_tree(NumOfLayers),
    Tree.

start_tree(NumOfLayers) ->
    receive
	{reducer_ref, ReducerRef, NumOfLayersLeft} ->
	    receive_layers([], NumOfLayers, 1);
	kill ->
	    killed_before_started_tree
    end.

receive_layers(Tree, NumOfLayers, 0) ->
    io:fwrite("Main: receive_layers has finished.~n",[]),
    Tree;
receive_layers(Tree, NumOfLayers, NumRunningReducers) ->
    receive
	{print, Msg, Args} ->
	    io:fwrite(Msg, Args),
	    receive_layers(Tree, NumOfLayers, NumRunningReducers);
	{add_node, Article, LinksToAdd, Layer} ->
	    Node = {Article, LinksToAdd, Layer},   % Might be bug in Layer, off by one bug.
	    receive_layers([Node|Tree], NumOfLayers, NumRunningReducers-1);
	{reducer_ref, ReducerRef, NumOfLayersLeft} ->
	    receive_layers(Tree, NumOfLayers, NumRunningReducers+1)
    end.

kill_mappers() ->
    [Mapper ! kill || Mapper <- get(mappers)],
    io:fwrite("Main: killed all mappers.~n",[]).

send_mapper(Parent, Article, NumOfLayers, Ref) ->
    Mapper = get_mappper(Article),
    Mapper ! {self(), Parent, Article, NumOfLayers-1}.

get_mappper(Article) ->
    NumOfMapper = erlang:phash(Article, length(get(mapper_pids))),
    MapperPid = lists:nth(NumOfMapper, get(mapper_pids)),
    MapperPid.
    
%%%% \Tested code %%%%%%
%%%%%%%%%%%%% \Main: %%%%%%%%%%%%%%%%%%%%%%%%%%%

create_test_database()->
    DB = ets:new(database, [set,public,named_table]),
    io:format("DB = [~p]~n", [DB]),
    %% ets:insert(DB, {0, [1,2,3]}).
    %%[X || X <- lists:seq(0,9), X /= 1],
    [%io:fwrite("Url ~p Links: [~p,~p,~p]~n", [Url,Link1,Link2,Link3]),
     ets:insert(DB, {Url, [Link1,Link2,Link3]}) ||
    	Url <- lists:seq(0,5),
    	Link1 <- lists:seq(0,5),
    	Link2 <- lists:seq(0,4),
    	Link3 <- lists:seq(0,3),
    	Url =/= Link1,
    	Url =/= Link2,
    	Url =/= Link3,
    	Link1 =/= Link2,
    	Link2 =/= Link3,
    	Link3 =/= Link1
     ].
