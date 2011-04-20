package org.handwerkszeug.riak.op;

import org.handwerkszeug.riak.RiakException;

/**
 * 
 * @author taichi
 * 
 * @param <T>
 */
public interface RiakResponseHandler<T> {

	void handle(RiakResponse<T> response) throws RiakException;
}
