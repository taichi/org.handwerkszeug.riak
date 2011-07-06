package org.handwerkszeug.riak.transport.internal;

import static org.handwerkszeug.riak.util.Validation.notNull;

import org.handwerkszeug.riak.RiakAction;
import org.handwerkszeug.riak.RiakClient;
import org.handwerkszeug.riak.config.RiakConfig;
import org.handwerkszeug.riak.op.BucketOperations;
import org.handwerkszeug.riak.op.ObjectKeyOperations;
import org.handwerkszeug.riak.op.Querying;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

/**
 * @author taichi
 */
public abstract class AbstractRiakClient<CONF extends RiakConfig, OP extends BucketOperations & ObjectKeyOperations & Querying & Completion>
		implements RiakClient<OP> {

	protected final CONF config;
	protected final ChannelPipelineFactory pipelineFactory;
	protected final ClientSocketChannelFactory channelFactory;

	public AbstractRiakClient(CONF config,
			ChannelPipelineFactory pipelineFactory) {
		this.channelFactory = new NioClientSocketChannelFactory(
				config.getBossExecutor(), config.getWorkerExecutor());
		this.pipelineFactory = pipelineFactory;
		this.config = config;
	}

	@Override
	public void execute(RiakAction<OP> action) {
		notNull(action, "action");
		// TODO stress test and implement connection pooling.
		ClientBootstrap bootstrap = new ClientBootstrap(this.channelFactory);
		Integer i = this.config.getTimeout();
		if (i != null) {
			bootstrap.setOption("connectTimeoutMillis", i);
		}

		bootstrap.setPipelineFactory(this.pipelineFactory);
		ChannelFuture future = bootstrap.connect(this.config.getRiakAddress());
		future = future.awaitUninterruptibly();
		Channel channel = future.getChannel();
		OP op = newOperations(channel);
		try {
			action.execute(op);
		} finally {
			op.complete();
		}
	}

	protected abstract OP newOperations(Channel channel);

	@Override
	public void dispose() {
		this.channelFactory.releaseExternalResources();
	}
}
