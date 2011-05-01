package org.handwerkszeug.riak.model;

/**
 * @author taichi
 */
public interface RiakContentsResponse<T> extends RiakResponse {

	/**
	 * @return
	 */
	T getResponse();
}
