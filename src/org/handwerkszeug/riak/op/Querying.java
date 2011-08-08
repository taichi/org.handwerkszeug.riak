package org.handwerkszeug.riak.op;

import org.handwerkszeug.riak.mapreduce.MapReduceQueryBuilder;
import org.handwerkszeug.riak.mapreduce.MapReduceResponse;
import org.handwerkszeug.riak.model.RiakFuture;

/**
 * @author taichi
 */
public interface Querying {

	/**
	 * only support json.
	 */
	static final String JobEncoding = "application/json";

	MapReduceQueryBuilder<RiakFuture> mapReduce(
			RiakResponseHandler<MapReduceResponse> handler);

	/**
	 * @see <a href="http://wiki.basho.com/MapReduce.html">MapReduce</a>
	 */
	RiakFuture mapReduce(String rawJson,
			RiakResponseHandler<MapReduceResponse> handler);
}
