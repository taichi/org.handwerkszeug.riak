package org.handwerkszeug.riak.http.rest;

import static org.handwerkszeug.riak.util.Validation.notNull;

import org.handwerkszeug.riak.Config;
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

/**
 * @author taichi
 */
public class RestRiakClient implements RiakClient<RestRiakOperations> {

	final Config config;
	final String riakUri;
	final ClientSocketChannelFactory channelFactory;

	public RestRiakClient(Config config) {
		this.config = config;
		this.channelFactory = new NioClientSocketChannelFactory(
				config.getBossExecutor(), config.getWorkerExecutor());
		this.riakUri = toRiakURI(config);
	}

	public static String toRiakURI(Config config) {
		StringBuilder stb = new StringBuilder();
		stb.append("http://");
		stb.append(config.getRiakAddress().getHostName());
		stb.append(':');
		stb.append(config.getRiakAddress().getPort());
		return stb.toString();
	}

	@Override
	public void execute(final RiakAction<RestRiakOperations> action) {
		notNull(action, "action");
		// TODO stress test and implement connection pooling.
		ClientBootstrap bootstrap = new ClientBootstrap(this.channelFactory);
		Integer i = config.getTimeout();
		if (i != null) {
			bootstrap.setOption("connectTimeoutMillis", i);
		}
		ChannelPipelineFactory pf = new RestPipelineFactory() {
			@Override
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline pipeline = super.getPipeline();
				pipeline.addFirst("clientHandler",
						new SimpleChannelUpstreamHandler() {
							@Override
							public void channelConnected(
									ChannelHandlerContext ctx,
									ChannelStateEvent e) throws Exception {
								RestRiakOperations op = new RestRiakOperations(
										riakUri, config, e.getChannel());
								action.execute(op);
							}
						});
				return pipeline;
			}
		};
		bootstrap.setPipelineFactory(pf);
		bootstrap.connect(this.config.getRiakAddress());
	}

	@Override
	public void dispose() {
		this.channelFactory.releaseExternalResources();
	}

}
