package org.handwerkszeug.riak.transport.rest;

import org.handwerkszeug.riak.Hosts;
import org.handwerkszeug.riak.RiakClient;
import org.handwerkszeug.riak.transport.internal.AbstractRiakClientTest;

/**
 * @author taichi
 */
public class RestRiakClientTest extends
		AbstractRiakClientTest<RestRiakOperations> {

	@Override
	protected RiakClient<RestRiakOperations> newTarget() {
		return new RestRiakClient(RestRiakConfig.newConfig(Hosts.RIAK_HOST,
				Hosts.RIAK_HTTP_PORT));
	}
}
