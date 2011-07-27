package org.handwerkszeug.riak.transport.protobuf;

import org.handwerkszeug.riak.Hosts;
import org.handwerkszeug.riak.ease.Riak;
import org.handwerkszeug.riak.ease.RiakTest;
import org.junit.Ignore;

public class ProtoBufRiakTest extends RiakTest<ProtoBufRiakOperations> {

	@Override
	protected Riak<ProtoBufRiakOperations> newTarget() {
		return ProtoBufRiak.create(Hosts.RIAK_HOST);
	}

	@Ignore
	@Override
	public void testPost() throws Exception {
		super.testPost();
	}
}
