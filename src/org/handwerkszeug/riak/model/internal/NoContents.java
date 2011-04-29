package org.handwerkszeug.riak.model.internal;

import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.RiakResponse;
import org.handwerkszeug.riak.nls.Messages;

/**
 * @author taichi
 */
public class NoContents<T> implements RiakResponse<T> {

	final Location location;

	public NoContents(Location location) {
		this.location = location;
	}

	@Override
	public boolean isErrorResponse() {
		return true;
	}

	@Override
	public String getMessage() {
		return String.format(Messages.NoContents, this.location);
	}

	@Override
	public int getResponseCode() {
		return 0;
	}

	@Override
	public T getResponse() {
		return null;
	}
}
