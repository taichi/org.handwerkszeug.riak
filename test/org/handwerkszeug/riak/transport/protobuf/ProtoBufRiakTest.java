package org.handwerkszeug.riak.transport.protobuf;

import org.handwerkszeug.riak.Hosts;
import org.handwerkszeug.riak.ease.Riak;
import org.handwerkszeug.riak.ease.RiakTest;

public class ProtoBufRiakTest extends RiakTest<ProtoBufRiakOperations> {

	@Override
	protected Riak<ProtoBufRiakOperations> newTarget() {
		return ProtoBufRiak.create(Hosts.RIAK_HOST);
	}
}
