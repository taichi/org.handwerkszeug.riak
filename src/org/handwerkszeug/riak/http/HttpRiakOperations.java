package org.handwerkszeug.riak.http;

import java.util.List;

import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.op.RiakOperations;
import org.handwerkszeug.riak.op.RiakResponse;

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
	 * @see <a
	 *      href="https://github.com/basho/riak_kv/blob/master/src/riak_kv_wm_link_walker.erl">riak_kv_wm_link_walker.erl</a>
	 */
	RiakResponse<List<RiakObject<byte[]>>> walk(Location walkbegin,
			List<LinkCondition> conditions);

}
