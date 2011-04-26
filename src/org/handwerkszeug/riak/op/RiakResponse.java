package org.handwerkszeug.riak.op;

/**
 * @author taichi
 */
public interface RiakResponse<T> {

	boolean isErrorResponse();

	String getMessage();

	int getResponseCode();

	T getResponse();
}
