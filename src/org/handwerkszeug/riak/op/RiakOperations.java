package org.handwerkszeug.riak.op;

import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.model.ServerInfo;

/**
 * @author taichi
 */
public interface RiakOperations extends BucketOperations, ObjectKeyOperations,
		Querying {

	/**
	 * Check if the server is alive
	 * 
	 * @param handler
	 * @return
	 */
	RiakFuture ping(RiakResponseHandler<_> handler);

	/**
	 * Get the client id used for this connection. Client ids are used for
	 * conflict resolution and each unique actor in the system should be
	 * assigned one. A client id is assigned randomly when the socket is
	 * connected and can be changed using SetClientId below.
	 * 
	 * @param handler
	 * @return
	 */
	RiakFuture getClientId(RiakResponseHandler<String> handler);

	/**
	 * Set the client id for this connection. A library may want to set the
	 * client id if it has a good way to uniquely identify actors across
	 * reconnects. This will reduce vector clock bloat.
	 * 
	 * @param id
	 * @param handler
	 * @return
	 */
	RiakFuture setClientId(String id, RiakResponseHandler<_> handler);

	/**
	 * get server information.
	 * 
	 * @param handler
	 * @return
	 */
	RiakFuture getServerInfo(RiakResponseHandler<ServerInfo> handler);
}
