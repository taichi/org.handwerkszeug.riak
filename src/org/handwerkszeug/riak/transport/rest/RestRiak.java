package org.handwerkszeug.riak.transport.rest;

import static org.handwerkszeug.riak.util.Validation.notNull;

import org.handwerkszeug.riak.ease.Riak;
import org.handwerkszeug.riak.model.Location;

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

	public LinkWalkingCommand walk(Location begin) {
		notNull(begin, "begin");
		return new LinkWalkingCommand(this.client, this.handler, begin);
	}
}
