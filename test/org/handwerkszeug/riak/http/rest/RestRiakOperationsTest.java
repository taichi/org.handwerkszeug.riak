package org.handwerkszeug.riak.http.rest;

import java.net.SocketAddress;

import org.handwerkszeug.riak.Hosts;
import org.handwerkszeug.riak.op.RiakOperations;
import org.handwerkszeug.riak.op.RiakOperationsTest;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipelineFactory;

public class RestRiakOperationsTest extends RiakOperationsTest {

	RestRiakOperations target;

	@Override
	protected ChannelPipelineFactory newChannelPipelineFactory() {
		return new RestPipelineFactory();
	}

	@Override
	protected RiakOperations newTarget(Channel channel) {
		this.target = new RestRiakOperations(Hosts.RIAK_URL, channel);
		return this.target;
	}

	@Override
	protected SocketAddress connectTo() {
		return Hosts.RIAK_HTTP_ADDR;
	}
}
