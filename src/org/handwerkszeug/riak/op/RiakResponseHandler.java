package org.handwerkszeug.riak.op;

import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.model.RiakResponse;

/**
 * 
 * @author taichi
 * 
 * @param <T>
 */
public interface RiakResponseHandler<T> {

	void onError(RiakResponse response) throws Exception;

	void handle(RiakContentsResponse<T> response) throws Exception;
}
