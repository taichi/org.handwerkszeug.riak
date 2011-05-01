package org.handwerkszeug.riak.model.internal;

import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.model.RiakObject;

/**
 * @author taichi
 */
public abstract class AbstractRiakObjectResponse extends AbstractRiakResponse
		implements RiakContentsResponse<RiakObject<byte[]>> {

	final RiakObject<byte[]> contents;

	public AbstractRiakObjectResponse(RiakObject<byte[]> contents) {
		this.contents = contents;
	}

	@Override
	public RiakObject<byte[]> getContents() {
		return this.contents;
	}
}
