package org.handwerkszeug.riak.op;

import org.handwerkszeug.riak.model.RiakFuture;

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
	RiakFuture ping(RiakResponseHandler<String> handler);

}
