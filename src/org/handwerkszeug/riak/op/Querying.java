package org.handwerkszeug.riak.op;

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
	void mapReduce(String requestJson); // XXX???
}
