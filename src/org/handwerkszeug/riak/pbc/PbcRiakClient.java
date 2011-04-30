package org.handwerkszeug.riak.pbc;

import static org.handwerkszeug.riak.util.Validation.notNull;

import java.net.SocketAddress;
import java.util.concurrent.Executors;

import org.handwerkszeug.riak.RiakAction;
import org.handwerkszeug.riak.RiakClient;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

public class PbcRiakClient implements RiakClient<PbcRiakOperations> {

	final SocketAddress remoteHost;

	final ClientSocketChannelFactory channelFactory;

	public PbcRiakClient(SocketAddress remoteHost) {
		// TODO read from configuration.
		this.channelFactory = new NioClientSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool());
		this.remoteHost = remoteHost;
	}

	@Override
	public void execute(final RiakAction<PbcRiakOperations> action) {
		notNull(action, "action");
		// TODO stress test and implement connection pooling.
		ClientBootstrap bootstrap = new ClientBootstrap(this.channelFactory);
		ChannelPipelineFactory pf = new PbcPipelineFactory() {
			@Override
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline pipeline = super.getPipeline();
				pipeline.addFirst("clientHandler",
						new SimpleChannelUpstreamHandler() {
							@Override
							public void channelConnected(
									ChannelHandlerContext ctx,
									ChannelStateEvent e) throws Exception {
								PbcRiakOperations op = new PbcRiakOperations(e
										.getChannel());
								action.execute(op);
							}
						});
				return pipeline;
			}
		};
		bootstrap.setPipelineFactory(pf);
		bootstrap.connect(this.remoteHost);
	}

	@Override
	public void dispose() {
		this.channelFactory.releaseExternalResources();
	}
}
