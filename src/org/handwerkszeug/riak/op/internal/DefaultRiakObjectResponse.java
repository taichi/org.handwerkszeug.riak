package org.handwerkszeug.riak.op.internal;

import org.handwerkszeug.riak.model.RiakObject;

/**
 * @author taichi
 */
public class DefaultRiakObjectResponse extends
		AbstractRiakResponse<RiakObject<byte[]>> {

	final RiakObject<byte[]> response;

	public DefaultRiakObjectResponse(RiakObject<byte[]> response) {
		this.response = response;
	}

	@Override
	public RiakObject<byte[]> getResponse() {
		return this.response;
	}
}
