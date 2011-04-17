package org.handwerkszeug.riak.op;

import org.handwerkszeug.riak.op.mapreduce.MapReduceQueryConstructor;
import org.handwerkszeug.riak.op.mapreduce.MapReduceResponseHandler;

/**
 * @author taichi
 */
public interface Querying {

	static final String JobEncoding = "application/json";

	/**
	 * @see <a href="http://wiki.basho.com/Links.html">Links</a>
	 */
	void link(String request); // XXX???

	/**
	 * @see <a href="http://wiki.basho.com/MapReduce.html">MapReduce</a>
	 */
	void mapReduce(MapReduceQueryConstructor constructor,
			MapReduceResponseHandler handler);
}
