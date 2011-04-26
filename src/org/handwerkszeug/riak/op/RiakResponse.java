package org.handwerkszeug.riak.op;

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
