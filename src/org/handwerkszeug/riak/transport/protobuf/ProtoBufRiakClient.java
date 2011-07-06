package org.handwerkszeug.riak.transport.protobuf;

import org.handwerkszeug.riak.transport.internal.AbstractRiakClient;
import org.handwerkszeug.riak.transport.protobuf.internal.ProtoBufPipelineFactory;
import org.jboss.netty.channel.Channel;

/**
 * @author taichi
 */
public class ProtoBufRiakClient extends
		AbstractRiakClient<ProtoBufRiakConfig, ProtoBufRiakOperations> {

	public ProtoBufRiakClient(ProtoBufRiakConfig config) {
		super(config, new ProtoBufPipelineFactory());
	}

	@Override
	protected ProtoBufRiakOperations newOperations(Channel channel) {
		return new ProtoBufRiakOperations(channel);
	}
}
