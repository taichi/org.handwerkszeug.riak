package org.handwerkszeug.riak.transport.internal;

import static org.handwerkszeug.riak.util.Validation.notNull;

import java.util.concurrent.Executor;

import org.handwerkszeug.riak.RiakAction;
import org.handwerkszeug.riak.RiakClient;
import org.handwerkszeug.riak.config.RiakConfig;
import org.handwerkszeug.riak.op.BucketOperations;
import org.handwerkszeug.riak.op.ObjectKeyOperations;
import org.handwerkszeug.riak.op.Querying;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.internal.ExecutorUtil;

/**
 * @author taichi
 */
public abstract class AbstractRiakClient<CONF extends RiakConfig, OP extends BucketOperations & ObjectKeyOperations & Querying & Completion>
		implements RiakClient<OP> {

	protected final CONF config;
	protected final ChannelPipelineFactory pipelineFactory;
	protected final ClientSocketChannelFactory channelFactory;

	protected final Executor actionExecutor;

	protected final Executor[] externalResources = new Executor[3];

	public AbstractRiakClient(CONF config,
			ChannelPipelineFactory pipelineFactory) {
		this.externalResources[0] = config.getActionExecutor();
		this.externalResources[1] = config.getBossExecutor();
		this.externalResources[2] = config.getWorkerExecutor();

		this.actionExecutor = this.externalResources[0];
		this.channelFactory = new NioClientSocketChannelFactory(
				this.externalResources[1], this.externalResources[2]);
		this.pipelineFactory = pipelineFactory;
		this.config = config;
	}

	@Override
	public void execute(final RiakAction<OP> action) {
		notNull(action, "action");
		// TODO stress test and implement connection pooling.
		ClientBootstrap bootstrap = new ClientBootstrap(this.channelFactory);
		Integer i = this.config.getTimeout();
		if (i != null) {
			bootstrap.setOption("connectTimeoutMillis", i);
		}

		bootstrap.setPipelineFactory(this.pipelineFactory);
		ChannelFuture future = bootstrap.connect(this.config.getRiakAddress());
		future.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future)
					throws Exception {
				if (future.isSuccess()) {
					final Channel channel = future.getChannel();
					AbstractRiakClient.this.actionExecutor
							.execute(new Runnable() {
								@Override
								public void run() {
									OP op = newOperations(channel);
									try {
										action.execute(op);
									} finally {
										op.complete();
									}
								}
							});
				}
			}
		});
	}

	protected abstract OP newOperations(Channel channel);

	@Override
	public void dispose() {
		ExecutorUtil.terminate(this.externalResources);
	}
}
