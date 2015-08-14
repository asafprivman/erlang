-module(node_computer).
-export([init_node/2]).
-define(Processors, erlang:system_info(logical_processors_available)).

%%%%%%%%%%%%% Node: %%%%%%%%%%%%%%%%%%%%%%%%%%%
init_node(NodeID, MainPid) ->
    MappedETS = ets:new(mapped_ets, [set,public]),
    MapperPids = create_mappers(NodeID),
    MainPid ! {mappers_created, NodeNum, MapperPids},
    MainPid ! {print, "init_node num ~p done.", [NodeID]}.

create_mappers(NodeID) ->
    [spawn(node_computer, create_mapper, [MapperID, NodeID]) || MapperID <- lists:seq(0,Processors-1)].

create_mapper(MapperID, NodeID) ->
    receive
	{start, MainPid, MapperIDs} ->
	    put(main_pid, MainPid), put(mapper_pids, MapperPids),
	    mapper(MapperID, NodeID)
    end.

mapper(MapperID, NodeID) ->
    receive
	{Sender, Parent, Article, NumOfLayersLeft} ->
	    map_if_not_mapped(is_mapped(Article), Sender, Parent, Article, NumOfLayersLeft),
	    mapper(MapperID, NodeID, NumOfLayersLeft);
	kill ->
	    get(main_pid) ! {print, "mapper num ~p node num ~p done(killed).", [MapperID, NodeID]};

is_mapped(Article) ->
    case ets:lookup(mapped_ets, Article) of
	[] ->
	    false;
	[_|_] ->                   % Might be bug here [_] or [_|_]
	    true
    end.


map_if_not_mapped(true, Sender, Parent, Article, NumOfLayersLeft) ->
    Sender ! {i_exist, Parent, Article};
map_if_not_mapped(false, Sender, Parent, Article, NumOfLayersLeft) ->
    ets:insert(mapped_ets, {Article, i_exist}),
    ReducerRef = erlang:make_ref(),
    get(main_pid) ! {reducer_ref, ReducerRef, NumOfLayersLeft},
    Sender ! {add_me, Parent, Article},
    spawn(node_computer, reducer, [Article, NumOfLayersLeft]).

reducer(Article, NumOfLayersLeft) ->
    Links = get_links_from_ets(Article), %V
    Ref = erlang:make_ref(),
    send_to_all_mappers(Article, Links, NumOfLayersLeft, Ref),
    LinksToAdd = Wait_for_mappers_answers(Links, [], Article),
    get(main_pid) ! {add_node, Article, LinksToAdd, NumOfLayersLeft}.

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
    send_mapper(Article, Link, NumOfLayersLeft, Ref),
    send_to_all_mappers(Article, T, NumOfLayersLeft, Ref).

Wait_for_mappers_answers([], LinksToAdd, MyArticle) ->
    LinksToAdd;
Wait_for_mappers_answers([Link|T], LinksToAdd, MyArticle) ->
    receive
	{i_exist, MyArticle, Link} ->
	    Wait_for_mappers_answers([Link|T], LinksToAdd);
	{add_me, MyArticle, Link} ->
	    Wait_for_mappers_answers([Link|T], [Link,LinksToAdd])
    end.

send_mapper(Parent, Article, NumOfLayers, Ref) ->
    Mapper = get_mappper(Article),
    Mapper ! {self(), Parent, Article, NumOfLayers-1}.

get_mappper(Article) ->
    NumOfMapper = erlang:phash(Article, length(get(mapper_pids))),
    MapperPid = lists:nth(NumOfMapper, get(mapper_pids)),
    MapperPid.
%%%%%%%%%%%%% \Node: %%%%%%%%%%%%%%%%%%%%%%%%%%%
