package org.handwerkszeug.riak.op.internal;

import org.handwerkszeug.riak.op.RiakResponse;

/**
 * @author taichi
 */
public abstract class AbstractRiakResponse<T> implements RiakResponse<T> {

	public AbstractRiakResponse() {
	}

	@Override
	public boolean isErrorResponse() {
		return false;
	}

	@Override
	public String getMessage() {
		return "";
	}

	@Override
	public int getResponseCode() {
		return 0;
	}
}
