package org.handwerkszeug.riak.mapreduce;

import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.JsonAppender;
import org.handwerkszeug.riak.model.Erlang;

/**
 * @author taichi
 * @see <a
 *      href="http://wiki.basho.com/Riak-Search---Querying.html#Querying-Integrated-with-Map-Reduce">Querying
 *      Integrated with Map/Reduce </a>
 * @see <a
 *      href="https://github.com/basho/riak_search/blob/master/apps/riak_search/src/riak_search.erl">riak_search.erl</a>
 */
public class MapReduceSearchInput implements JsonAppender<ObjectNode> {

	static final Erlang riakSearch = new Erlang("riak_search", "mapred_search");

	final String index;
	final String query;

	public MapReduceSearchInput(String index, String query) {
		this.index = index;
		this.query = query;
	}

	@Override
	public void appendTo(ObjectNode json) {
		riakSearch.appendTo(json);
		ArrayNode arg = json.putArray("arg");
		arg.add(this.index);
		arg.add(this.query);
	}

}
