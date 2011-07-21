package org.handwerkszeug.riak.transport.rest;

import org.handwerkszeug.riak.Hosts;
import org.handwerkszeug.riak.ease.Riak;
import org.handwerkszeug.riak.ease.RiakTest;

public class RestRiakTest extends RiakTest<RestRiakOperations> {

	@Override
	protected Riak<RestRiakOperations> newTarget() {
		return RestRiak.create(Hosts.RIAK_HOST);
	}
}
