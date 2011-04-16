package org.handwerkszeug.riak.op;

public interface Querying {

	static final String JobEncoding = "application/json";

	/**
	 * @see <a href="http://wiki.basho.com/MapReduce.html">MapReduce</a>
	 */
	void mapReduce(String requestJson);
}
