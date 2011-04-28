package org.handwerkszeug.riak.model;

/**
 * @author taichi
 */
public interface RiakResponse<T> {

	/**
	 * @return true : contains error. and {@link #getResponse()} throws
	 *         {@code UnsupportedOperationException}
	 */
	boolean isErrorResponse();

	String getMessage();

	int getResponseCode();

	/**
	 * @return
	 * @throws UnsupportedOperationException
	 */
	T getResponse();
}
