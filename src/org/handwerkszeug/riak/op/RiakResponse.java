package org.handwerkszeug.riak.op;

public interface RiakResponse<T> {

	boolean isErrorResponse();

	String getErrorMessage();

	int getErrorCode();

	T getResponse();
}
