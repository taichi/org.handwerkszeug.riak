package org.handwerkszeug.riak.op;

import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.model.ServerInfo;

/**
 * @author taichi
 * @see <a href="http://wiki.basho.com/PBC-API.html">PBC API</a>
 * @see <a href="http://wiki.basho.com/REST-API.html">REST API</a>
 * @see <a
 *      href="https://github.com/basho/riak_kv/blob/master/src/riak_kv_wm_raw.erl">Riak
 *      REST Server code</a>
 * @see <a
 *      href="https://github.com/basho/riak_kv/blob/master/src/riak_kv_pb_socket.erl">Riak
 *      Protocol Buffers Server</a>
 */
public interface RiakOperations extends BucketOperations, ObjectKeyOperations,
		Querying {

	RiakResponse<_> ping();

	RiakResponse<String> getClientId();

	RiakResponse<_> setClientId(String id);

	RiakResponse<ServerInfo> serverInfo();
}
