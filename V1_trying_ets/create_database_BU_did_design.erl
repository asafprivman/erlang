-module(create_database).
-export([start/0,lookup/0,print_item/1]).

start()->
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

%% plan:
%% create a test:
%%        add a graph of vertices with lots of edges (not a tree).
%%        run the map reduce.
%%        print the tree.
%%        assert certain checks.
%% mapreduce

TODO:
create func
add ,.;
check connections bwtween sender receivers same expected format
check % "% Maybe" bugs

V1:
%%%%%%%%%%%%% Main: %%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%% Test Bench %%%%%
test():
NumOfNodes = 3
NumOfLayers = 5
create_nodes_for_test(NumOfNodes) %V
Tree = init_main(NumOfNodes, NumOfLayers)%V
print Tree
print "Note to self, check tree correct add an assert ExpectedTree = Tree"

create_nodes_for_test(NumOfNodes):
for NodeID in NumOfNodes: 
spawn(init_node(NodeID, MainPid))%V

%%%% Tested code %%%%%
init_main(NumOfNodes, NumOfLayers):
send_ets_to_nodes()%V
Tree = create_tree(RootArticle, NumOfLayers)%V
kill_left_mappers()%V
return Tree


%TODO:send ets
send_ets_to_nodes():
%TODO:send ets
Mappers = receive_mappers()%V
send_mappers_all_mappers(Mappers)%V
put(mappers,Mappers)


create_tree(RootArticle, NumOfLayers):
Ref = erlang:make_ref(),
SendMapper(root, RootArticle, NumOfLayers, Ref)%V
Tree = receive_layers([], NumOfLayers)%V
Tree.


SendMapper(Parent, Article, NumOfLayers, Ref) ->
    Mapper = GetMappper(Article),
    Mapper ! {self(), Parent, Article, NumOfLayers-1}.

receive_layers(Tree, NumOfLayers):
receive
    {print} 
    receive_layers(Tree, NumOfLayers)
    {add_node}
    receive_layers([node|Tree], NumOfLayers-1)

%%%% \Tested code %%%%%%
%%%%%%%%%%%%% \Main: %%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%% Node: %%%%%%%%%%%%%%%%%%%%%%%%%%%
init_node(NodeID, MainPid):
MapperIDs = create_mappers(NodeID)%V
MainPid ! {mappers_created, NodeNum, MapperIDs}
receive_save_all_mappers()%V
MainPid ! {print, "init_node num ~p done.", [NodeID]}

create_mappers(NodeID):
for num of cores create mappers:
spawn(create_mapper(MapperID, NodeID))
MapperIDs.

create_mapper(MapperID, NodeID):
receive
    {start, MainPid, MapperIDs} ->
	put(main_pid, MainPid), put(mapper_IDs, MapperIDs),
	mapper(MapperID, NodeID)

mapper(MapperID, NodeID) ->
    get(main_pid) ! {print, "mapper num ~p node num ~p done.", [MapperID, NodeID]},
    mapper_zombie_until_all_reducers_die(MapperID, NodeID); % Might be bug here implement.
mapper(MapperID, NodeID) ->
    receive
	{Sender, Parent, Article, NumOfLayersLeft} ->
	    map_if_not_mapped(is_mapped(Article), Sender, Parent, Article, NumOfLayersLeft),
	    mapper(MapperID, NodeID, NumOfLayersLeft);
	kill
	get(main_pid) ! {print, "mapper num ~p node num ~p killed.", [MapperID, NodeID]};

is_mapped(Article) ->
    case ets:lookup(Article) of
	[] ->
	    false
	[_] ->                      % Might be bug here [_] or [_|_]
	    true

map_if_not_mapped(true, Sender, Parent, Article, NumOfLayersLeft) ->
    Sender ! {i_exist, Parent, Article};
map_if_not_mapped(false, Sender, Parent, Article, NumOfLayersLeft) ->
    add_to_local_ets(),
    Sender ! {add_me, Parent, Article},
    spawn(reducer(Article, NumOfLayersLeft)).

reducer(Article, NumOfLayersLeft):
Links = get_links_from_ets(Article), %V
Ref = erlang:make_ref(),
send_to_all_mappers(Article, Links, NumOfLayersLeft, Ref),
Wait_for_mappers_answers(Links),
get(main_pid) ! {add_node, Article, links}.

get_links_from_ets(Article) ->
    case ets:lookup(database, Article) of % Might be bug here
	[]->
	    io:fwrite("Error didn't find article:[~p] in database.~n",[Article]),
	    [];
	[{Article,Links}]->
	    Links
    end.


send_to_all_mappers(_, [], _, _) ->
    done;
send_to_all_mappers(Article, [Link|T], NumOfLayersLeft, Ref) ->
    SendMapper(Article, Link, NumOfLayersLeft, Ref),
    send_to_all_mappers(Article, T, NumOfLayersLeft, Ref).

Wait_for_mappers_answers([Link|T]) ->
    
gather(0, _, L) -> L;
gather(N, Ref, L) ->
    receive
	{Ref, Ret} -> gather(N-1, Ref, [Ret|L])
    end.
    

%%%%%%%%%%%%% \Node: %%%%%%%%%%%%%%%%%%%%%%%%%%%

implement:
Main:
receive_mappers()
send_mappers_all_mappers(Mappers)
GetMappper(Article) %hash the art for nth mapper get id
kill_left_mappers()

Node:
receive_save_all_mappers()
add_to_local_ets()
get_links_from_ets(Article)
Wait_for_mappers_answers()



Start from root:
Map (root, x)

Map (father, x):
	If (x in tree): exit
Add x to tree
Create link from father to x in tree
Create new reducer per new x given reducer (x)
	Reduce (x):
		GetValuesFromDB (x)
		For val in vals:
			Mapper = phash(val)
			Mapper ! {x, val)




create_tree() ->
    ReducerIDs = lists:seq(1,erlang:system_info(logical_processors_available)),
    pmap(fun create_reducer/1, ReducerIDs).

    %% NumOfReducers = erlang:system_info(logical_processors_available),
    %% put(reducers, [spawn(fun() -> create_user(ID) end) || ID <- ReducerIDs]),
    %% create_mapper().

create_reducer(ID) ->
    pass.

lookup() ->
    print_item(0).

print_item(ItemKey) ->
    case ets:lookup(database, ItemKey) of
        [{Key, Value}] ->
            io:format("Key ~p~n", [Key]),
            io:format("Value ~p~n", [Value]);
        [] ->
            io:format("No item with key = ~p~n", [ItemKey])
    end.


% pmap, order is not important
% pmap(reducer function, List of inputs for the reducer function)
pmap(F, L) ->
    S = self(),
    Ref = erlang:make_ref(),
    lists:foreach(fun(I) ->
			  spawn(fun() -> do_f(S, Ref, F, I) end)
		  end, L),
    
    %startTree

    %% gather the results
    gather(length(L), Ref, []).

do_f(Parent, Ref, F, I) ->
    Parent ! {Ref, (catch F(I))}.
gather(0, _, L) -> L;
gather(N, Ref, L) ->
    receive
	{Ref, Ret} -> gather(N-1, Ref, [Ret|L])
    end.
