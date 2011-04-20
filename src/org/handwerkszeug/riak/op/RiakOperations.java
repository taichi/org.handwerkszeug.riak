package org.handwerkszeug.riak.op;

import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.model.ServerInfo;

/**
 * @see <a href="http://wiki.basho.com/PBC-API.html">PBC API</a>
 * @see <a
 *      href="https://github.com/basho/riak_kv/blob/master/src/riak_kv_pb_socket.erl">Riak
 *      Protocol Buffers Server</a>
 * @author taichi
 */
public interface RiakOperations extends BucketOperations, ObjectKeyOperations,
		Querying {

	RiakFuture ping(RiakResponseHandler<_> handler);

	RiakFuture getClientId(RiakResponseHandler<String> handler);

	RiakFuture setClientId(String id, RiakResponseHandler<_> handler);

	RiakFuture serverInfo(RiakResponseHandler<ServerInfo> handler);
}
