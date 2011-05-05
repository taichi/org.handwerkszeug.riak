package org.handwerkszeug.riak.http;

import java.util.List;

import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.PutOptions;
import org.handwerkszeug.riak.model.RiakFuture;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.op.RiakOperations;
import org.handwerkszeug.riak.op.RiakResponseHandler;

/**
 * <p>
 * not support manual sibling.<br/>
 * when you use http client and {@code get} operations,<br/>
 * client set {@code Accept : multipart/mixed} all time.<br/>
 * </p>
 * 
 * @author taichi
 * @see <a href="http://wiki.basho.com/REST-API.html">REST API</a>
 * @see <a href
 *      ="https://github.com/basho/riak_kv/blob/master/src/riak_kv_wm_raw.erl"
 *      >Riak REST Server code</a>
 */
public interface HttpRiakOperations extends RiakOperations, LuwakSupport {

	/**
	 * All requests should include the “X-Riak-ClientId” header, which can be
	 * any string that uniquely identifies the client, for purposes of tracing
	 * object modifications in the vector clock.<br/>
	 * if you set clientId null, don't send clientId.
	 * 
	 * @param clientId
	 */
	void setClientId(String clientId);

	String getClientId();

	/**
	 * Stores a new object in a bucket with a random Riak-assigned key.<br/>
	 * set Location key as empty string.
	 */
	RiakFuture post(RiakObject<byte[]> content,
			RiakResponseHandler<RiakObject<byte[]>> handler);

	/**
	 * Stores a new object in a bucket with a random Riak-assigned key.<br/>
	 * set Location key as empty string.
	 */
	RiakFuture post(RiakObject<byte[]> content, PutOptions options,
			RiakResponseHandler<RiakObject<byte[]>> handler);

	/**
	 * notify messages for every steps.
	 * 
	 * @see <a href="http://wiki.basho.com/Links.html">Links</a>
	 * @see <a
	 *      href="http://blog.basho.com/2010/02/24/link-walking-by-example/">Link
	 *      Walking By Example </a>
	 * @see <a
	 *      href="https://github.com/basho/riak_kv/blob/master/src/riak_kv_wm_link_walker.erl">riak_kv_wm_link_walker.erl</a>
	 */
	RiakFuture walk(Location walkbegin, List<LinkCondition> conditions,
			RiakResponseHandler<List<RiakObject<byte[]>>> handler);

	/**
	 * @see <a href="http://wiki.basho.com/REST-API.html#Other-operations">Other
	 *      operations</a>
	 */
	RiakFuture getStats(RiakResponseHandler<ObjectNode> handler);
}
