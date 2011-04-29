package org.handwerkszeug.riak.op;

import org.handwerkszeug.riak.mapreduce.MapReduceQueryConstructor;
import org.handwerkszeug.riak.mapreduce.MapReduceResponse;

/**
 * @author taichi
 */
public interface Querying {

	/**
	 * only support json.
	 */
	static final String JobEncoding = "application/json";

	/**
	 * @see <a href="http://wiki.basho.com/MapReduce.html">MapReduce</a>
	 */
	void mapReduce(MapReduceQueryConstructor constructor,
			RiakResponseHandler<MapReduceResponse> handler);
}
