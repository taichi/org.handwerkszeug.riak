package org.handwerkszeug.riak.op;

import org.handwerkszeug.riak.mapreduce.MapReduceQueryConstructor;
import org.handwerkszeug.riak.mapreduce.MapReduceResponseHandler;

/**
 * @author taichi
 */
public interface Querying {

	/**
	 * only support json.
	 */
	static final String JobEncoding = "application/json";

	/**
	 * @see <a href="http://wiki.basho.com/Links.html">Links</a>
	 */
	void link(String request); // TODO ???

	/**
	 * @see <a href="http://wiki.basho.com/MapReduce.html">MapReduce</a>
	 */
	void mapReduce(MapReduceQueryConstructor constructor,
			MapReduceResponseHandler handler);
}
