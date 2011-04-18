package org.handwerkszeug.riak.op;

/**
 * @author taichi
 */
public interface RiakResponse<T> {

	boolean isErrorResponse();

	String getErrorMessage();

	int getErrorCode();

	T getResponse();
}
