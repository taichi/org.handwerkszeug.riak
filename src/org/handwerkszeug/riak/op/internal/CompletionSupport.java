package org.handwerkszeug.riak.op.internal;

import static org.handwerkszeug.riak.util.Validation.notNull;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
	final AtomicInteger progress = new AtomicInteger(0);
	final AtomicBoolean complete = new AtomicBoolean(false);

	public CompletionSupport(Channel channel) {
		notNull(channel, "channel");
		this.channel = channel;
	}

	@Override
	public void operationComplete(ChannelFuture future) throws Exception {
		if (future.isDone() && this.progress.get() < 1 && this.complete.get()) {
			LOG.debug(Markers.BOUNDARY, Messages.CLOSE_CHANNEL);
			future.getChannel().close();
		}
	}

	public void complete() {
		this.complete.compareAndSet(false, true);
	}

	public <T> RiakFuture handle(final String name, Object send,
			final RiakResponseHandler<T> users,
			final NettyUtil.MessageHandler handler) {
		if (LOG.isDebugEnabled()) {
			LOG.debug(name);
		}
		this.progress.incrementAndGet();
		ChannelPipeline pipeline = this.channel.getPipeline();
		pipeline.addLast(name, new UpstreamHandler<T>(users) {
			@Override
			public void messageReceived(ChannelHandlerContext ctx,
					MessageEvent e) throws Exception {
				ChannelPipeline pipeline = e.getChannel().getPipeline();
				try {
					Object receive = e.getMessage();
					if (LOG.isDebugEnabled()) {
						LOG.debug(Markers.DETAIL, Messages.Receive, name,
								receive);
					}
					if (handler.handle(receive)) {
						pipeline.remove(name);
						progress.decrementAndGet();
					}
					e.getFuture().addListener(CompletionSupport.this);
				} catch (Exception ex) {
					pipeline.remove(name);
					throw ex;
				}
			}
		});

		try {
			ChannelFuture cf = this.channel.write(send);
			return new NettyUtil.FutureAdapter(cf);
		} catch (Exception e) {
			pipeline.remove(name);
			complete();
			this.channel.close();
			throw new RiakException(e);
		}
	}

	class UpstreamHandler<T> extends SimpleChannelUpstreamHandler {
		final RiakResponseHandler<T> users;

		public UpstreamHandler(RiakResponseHandler<T> users) {
			this.users = users;
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx,
				final ExceptionEvent e) throws Exception {
			users.onError(new AbstractRiakResponse() {
				@Override
				public String getMessage() {
					return e.getCause().getMessage();
				}

				@Override
				public void operationComplete() {
					complete();
				}
			});
			LOG.error(e.getCause().getMessage(), e.getCause());
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
