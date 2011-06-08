package org.handwerkszeug.riak.transport.internal;

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
import org.handwerkszeug.riak.model.AbstractRiakResponse;
import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.model.RiakFuture;
import org.handwerkszeug.riak.nls.Messages;
import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.handwerkszeug.riak.util.NettyUtil;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
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

	public void remove(String name) {
		this.inProgress.remove(name);
	}

	public CountDownRiakFuture newRiakFuture(String name) {
		ChannelPipeline pipeline = this.channel.getPipeline();
		return new CountDownRiakFuture(name, pipeline);
	}

	public <T> RiakFuture handle(String name, Object send,
			RiakResponseHandler<T> users, NettyUtil.MessageHandler handler) {
		CountDownRiakFuture future = newRiakFuture(name);
		ChannelHandler ch = new DefaultCompletionChannelHandler<T>(this, name,
				users, handler, future);
		return handle(name, send, users, ch, future);
	}

	public <T> RiakFuture handle(String name, Object send,
			RiakResponseHandler<T> users, ChannelHandler handler,
			RiakFuture future) {
		if (LOG.isDebugEnabled()) {
			LOG.debug(Messages.SendTo, name, this.channel.getRemoteAddress());
		}
		ChannelPipeline pipeline = this.channel.getPipeline();

		Command cmd = new Command(this.channel, send, name, handler);
		try {
			if (this.inProgress.add(cmd.name)) {
				cmd.execute();
			} else {
				this.waitQueue.add(cmd);
			}
			return future;
		} catch (Exception e) {
			complete(name, pipeline);
			throw new RiakException(e);
		} catch (Error e) {
			complete(name, pipeline);
			throw e;
		}
	}

	protected void complete(String name, ChannelPipeline pipeline) {
		pipeline.remove(name);
		complete();
		this.channel.close();
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
