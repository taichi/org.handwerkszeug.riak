package org.handwerkszeug.riak.pbc;

import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.handwerkszeug.riak.Hosts;
import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.op.RiakResponse;
import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.junit.Before;
import org.junit.Test;

public class PbcRiakOperationsTest {

	ClientBootstrap bootstrap;
	Channel channel;
	PbcRiakOperations target;

	@Before
	public void setUp() throws Exception {
		this.bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));
		this.bootstrap.setPipelineFactory(new PbcPipelineFactory());
		ChannelFuture future = this.bootstrap.connect(new InetSocketAddress(
				Hosts.RIAK_HOST, Hosts.RIAK_PB_PORT));
		this.channel = future.awaitUninterruptibly().getChannel();

		this.target = new PbcRiakOperations(this.channel);
	}

	public void tearDown() throws Exception {
		this.channel.close().awaitUninterruptibly();
		this.bootstrap.releaseExternalResources();
	}

	@Test
	public void testPing() throws Exception {
		this.target.ping(new RiakResponseHandler<_>() {
			@Override
			public void handle(RiakResponse<_> response) throws RiakException {
				assertEquals("pong", response.getMessage());
			}
		});
	}

}
