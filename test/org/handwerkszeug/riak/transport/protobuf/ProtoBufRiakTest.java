package org.handwerkszeug.riak.transport.protobuf;

import org.handwerkszeug.riak.Hosts;
import org.handwerkszeug.riak.ease.RiakTest;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author taichi
 */
public class ProtoBufRiakTest extends
		RiakTest<ProtoBufRiakOperations, ProtoBufRiak> {

	@Override
	protected ProtoBufRiak newTarget() {
		return ProtoBufRiak.create(Hosts.RIAK_HOST);
	}

	@Override
	@Test
	@Ignore("Riak 0.14.2 has bug. fix that bug in the future")
	public void testPost() throws Exception {
		super.testPost();
	}
}
