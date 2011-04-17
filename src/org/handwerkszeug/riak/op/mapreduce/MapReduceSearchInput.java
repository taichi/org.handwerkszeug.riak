package org.handwerkszeug.riak.op.mapreduce;

import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.JsonAppender;
import org.handwerkszeug.riak.model.Erlang;

/**
 * 
 * @author taichi
 * @see <a
 *      href="http://wiki.basho.com/Riak-Search---Querying.html#Querying-Integrated-with-Map-Reduce">Querying
 *      Integrated with Map/Reduce </a>
 */
public class MapReduceSearchInput implements JsonAppender<ObjectNode> {

	static final Erlang riakSearch = new Erlang("riak_search", "mapred_search");

	final String bucket;
	final String query;

	public MapReduceSearchInput(String bucket, String query) {
		this.bucket = bucket;
		this.query = query;
	}

	@Override
	public void appendTo(ObjectNode json) {
		json.put("module", riakSearch.getModule());
		json.put("function", riakSearch.getFunction());
		ArrayNode arg = json.putArray("arg");
		arg.add(this.bucket);
		arg.add(this.query);
	}

}
