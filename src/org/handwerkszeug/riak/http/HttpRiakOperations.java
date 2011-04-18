package org.handwerkszeug.riak.http;

import org.handwerkszeug.riak.op.RiakOperations;

/**
 * <p>
 * not support manual sibling.<br/>
 * when you use http client and {@code get} operations,<br/>
 * client set {@code Accept : multipart/mixed} all time.<br/>
 * </p>
 * 
 * @see <a href="http://wiki.basho.com/REST-API.html">REST API</a>
 * 
 * @see <a href
 *      ="https://github.com/basho/riak_kv/blob/master/src/riak_kv_wm_raw.erl"
 *      >Riak REST Server code</a>
 * @author taichi
 */
public interface HttpRiakOperations extends RiakOperations, LuwakSupport {

	/**
	 * @see <a href="http://wiki.basho.com/Links.html">Links</a>
	 * @see <a
	 *      href="http://blog.basho.com/2010/02/24/link-walking-by-example/">Link
	 *      Walking By Example </a>
	 */
	void link(String request);

	/**
	 * @see <a href="http://wiki.basho.com/Riak-Search---Querying.html">Riak
	 *      Search Querying</a>
	 */
	void search(String query);
}
