package org.handwerkszeug.riak.transport.protobuf;

import org.handwerkszeug.riak.ease.Riak;

/**
 * @author taichi
 */
public class ProtoBufRiak extends Riak<ProtoBufRiakOperations> {

	public ProtoBufRiak(ProtoBufRiakClient client) {
		super(client);
	}

	public ProtoBufRiak(ProtoBufRiakConfig config) {
		this(new ProtoBufRiakClient(config));
	}

	public static ProtoBufRiak create(String host) {
		return new ProtoBufRiak(ProtoBufRiakConfig.newConfig(host));
	}
}
