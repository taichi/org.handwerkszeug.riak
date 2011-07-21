package org.handwerkszeug.riak.transport.rest;

import org.handwerkszeug.riak.ease.Riak;

/**
 * @author taichi
 */
public class RestRiak extends Riak<RestRiakOperations> {

	public RestRiak(RestRiakConfig config) {
		super(new RestRiakClient(config));
	}

	public static RestRiak create(String host) {
		return new RestRiak(RestRiakConfig.newConfig(host));
	}
}
