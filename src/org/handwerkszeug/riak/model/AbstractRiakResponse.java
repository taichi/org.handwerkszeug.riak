package org.handwerkszeug.riak.model;


/**
 * @author taichi
 */
public abstract class AbstractRiakResponse implements RiakResponse {

	public AbstractRiakResponse() {
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
