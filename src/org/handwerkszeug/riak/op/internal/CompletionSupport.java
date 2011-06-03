package org.handwerkszeug.riak.op.internal;

import static org.handwerkszeug.riak.util.Validation.notNull;

import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.handwerkszeug.riak.Markers;
import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.model.RiakFuture;
import org.handwerkszeug.riak.model.internal.AbstractRiakResponse;
import org.handwerkszeug.riak.nls.Messages;
import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.handwerkszeug.riak.util.NettyUtil;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author taichi
 */
public class CompletionSupport implements ChannelFutureListener {

	static Logger LOG = LoggerFactory.getLogger(CompletionSupport.class);

	final Channel channel;

	final Set<String> inProgress = new ConcurrentSkipListSet<String>();
	final Queue<Command> waitQueue = new ConcurrentLinkedQueue<Command>();
	final AtomicBoolean complete = new AtomicBoolean(false);

	public CompletionSupport(Channel channel) {
		notNull(channel, "channel");
		this.channel = channel;
	}

	@Override
	public void operationComplete(ChannelFuture future) throws Exception {
		if (future.isDone() && this.waitQueue.size() < 1
				&& this.inProgress.size() < 1 && this.complete.get()) {
			LOG.debug(Markers.BOUNDARY, Messages.CloseChannel);
			future.getChannel().close();
		}
	}

	public void complete() {
		this.complete.compareAndSet(false, true);
	}

	public <T> RiakFuture handle(String name, Object send,
			final RiakResponseHandler<T> users,
			final NettyUtil.MessageHandler handler) {
		if (LOG.isDebugEnabled()) {
			LOG.debug(Messages.SendTo, name, this.channel.getRemoteAddress());
		}

		ChannelPipeline pipeline = this.channel.getPipeline();
		CountDownRiakFuture future = new CountDownRiakFuture(name, pipeline);

		Command cmd = new Command(this.channel, send, name,
				new UpstreamHandler<T>(name, users, handler, future));
		try {
			if (this.inProgress.add(cmd.name)) {
				cmd.execute();
			} else {
				this.waitQueue.add(cmd);
			}
			return future;
		} catch (Exception e) {
			pipeline.remove(name);
			complete();
			this.channel.close();
			throw new RiakException(e);
		}
	}

	protected void invokeNext() {
		for (Iterator<Command> i = this.waitQueue.iterator(); i.hasNext();) {
			Command cmd = i.next();
			if (this.inProgress.add(cmd.name)) {
				i.remove();
				// TODO message
				LOG.debug(Markers.DETAIL, "invoke Next {}", cmd.name);
				cmd.execute();
			}
		}
	}

	class UpstreamHandler<T> extends SimpleChannelUpstreamHandler {
		final String name;
		final RiakResponseHandler<T> users;
		final NettyUtil.MessageHandler handler;
		final CountDownRiakFuture future;

		public UpstreamHandler(String name, RiakResponseHandler<T> users,
				NettyUtil.MessageHandler handler, CountDownRiakFuture future) {
			this.name = name;
			this.users = users;
			this.handler = handler;
			this.future = future;
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx,
				final ExceptionEvent e) throws Exception {
			CompletionSupport.this.inProgress.remove(this.name);
			this.future.setFailure(e.getCause());
			LOG.error(e.getCause().getMessage(), e.getCause());
			this.users.onError(new AbstractRiakResponse() {
				@Override
				public String getMessage() {
					return e.getCause().getMessage();
				}

				@Override
				public void operationComplete() {
					complete();
				}
			});

			invokeNext();
		}

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
				throws Exception {
			try {
				Object receive = e.getMessage();
				if (LOG.isDebugEnabled()) {
					LOG.debug(Markers.DETAIL, Messages.Receive, this.name,
							receive);
				}
				if (this.handler.handle(receive)) {
					this.future.setSuccess();
					CompletionSupport.this.inProgress.remove(this.name);
					invokeNext();
				}
				e.getFuture().addListener(CompletionSupport.this);
			} catch (Exception ex) {
				LOG.error(ex.getMessage(), ex);
				this.future.setFailure(ex);
				invokeNext();
				throw ex;
			}
		}
	}

	public RiakContentsResponse<_> newResponse() {
		return newResponse(_._);
	}

	public <T> RiakContentsResponse<T> newResponse(final T contents) {
		return new AbstractCompletionRiakResponse<T>() {
			@Override
			public T getContents() {
				return contents;
			}
		};
	}

	public abstract class AbstractCompletionRiakResponse<T> extends
			AbstractRiakResponse implements RiakContentsResponse<T> {
		@Override
		public void operationComplete() {
			complete();
		}
	}
}
