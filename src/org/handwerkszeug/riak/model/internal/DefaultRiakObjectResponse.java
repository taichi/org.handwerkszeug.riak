package org.handwerkszeug.riak.model.internal;

import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.model.RiakObject;

/**
 * @author taichi
 */
public abstract class DefaultRiakObjectResponse extends AbstractRiakResponse
		implements RiakContentsResponse<RiakObject<byte[]>> {

	final RiakObject<byte[]> response;

	public DefaultRiakObjectResponse(RiakObject<byte[]> response) {
		this.response = response;
	}

	@Override
	public RiakObject<byte[]> getContents() {
		return this.response;
	}
}
