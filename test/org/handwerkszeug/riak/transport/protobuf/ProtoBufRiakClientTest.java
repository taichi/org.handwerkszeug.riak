package org.handwerkszeug.riak.transport.protobuf;

import org.handwerkszeug.riak.Hosts;
import org.handwerkszeug.riak.RiakClient;
import org.handwerkszeug.riak.transport.internal.AbstractRiakClientTest;

/**
 * @author taichi
 */
public class ProtoBufRiakClientTest extends
		AbstractRiakClientTest<ProtoBufRiakOperations> {

	@Override
	protected RiakClient<ProtoBufRiakOperations> newTarget() {
		return new ProtoBufRiakClient(
				ProtoBufRiakConfig.newConfig(Hosts.RIAK_HOST));
	}

}
