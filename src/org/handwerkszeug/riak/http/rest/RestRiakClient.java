package org.handwerkszeug.riak.http.rest;

import static org.handwerkszeug.riak.util.Validation.notNull;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Executors;

import org.handwerkszeug.riak.RiakAction;
import org.handwerkszeug.riak.RiakClient;
import org.handwerkszeug.riak.RiakException;
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

	final String riakUri;
	final SocketAddress remoteHost;
	final ClientSocketChannelFactory channelFactory;

	public RestRiakClient(String riakUri) {
		this.channelFactory = new NioClientSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool());
		this.riakUri = riakUri;
		try {
			URI uri = new URI(riakUri);
			String host = uri.getHost();
			int port = uri.getPort() < 0 ? 8080 : uri.getPort();
			this.remoteHost = new InetSocketAddress(host, port);
		} catch (URISyntaxException e) {
			throw new RiakException(e);
		}

	}

	@Override
	public void execute(final RiakAction<RestRiakOperations> action) {
		notNull(action, "action");
		// TODO stress test and implement connection pooling.
		ClientBootstrap bootstrap = new ClientBootstrap(this.channelFactory);
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
										riakUri, "riak", e.getChannel());
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
