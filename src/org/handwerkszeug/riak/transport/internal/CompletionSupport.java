package org.handwerkszeug.riak.transport.internal;

import static org.handwerkszeug.riak.util.Validation.notNull;

import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.handwerkszeug.riak.Markers;
import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.model.RiakFuture;
import org.handwerkszeug.riak.nls.Messages;
import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author taichi
 */
public class CompletionSupport {

	static Logger LOG = LoggerFactory.getLogger(CompletionSupport.class);

	final Channel channel;

	final Set<String> inProgress = new ConcurrentSkipListSet<String>();
	final Queue<OperationTask> waitQueue = new ConcurrentLinkedQueue<OperationTask>();
	final AtomicBoolean operationComplete = new AtomicBoolean(false);
	final ReentrantLock lock = new ReentrantLock();

	public CompletionSupport(Channel channel) {
		notNull(channel, "channel");
		this.channel = channel;
	}

	public void responseComplete() {
		complete();
	}

	public void operationComplete() {
		this.operationComplete.compareAndSet(false, true);
		complete();
	}

	protected void complete() {
		if (LOG.isDebugEnabled()) {
			LOG.debug(
					Markers.DETAIL,
					"queue:{} inprocess:{} complete:{}",
					new Object[] { this.waitQueue.size(),
							this.inProgress.size(),
							this.operationComplete.get() });
		}
		if (this.operationComplete.get() && this.waitQueue.size() < 1
				&& this.inProgress.size() < 1) {
			close(this.channel);
		}
	}

	protected void close(Channel channel) {
		if (this.lock.tryLock()) {
			try {
				if (channel.isOpen()) {
					LOG.debug(Markers.BOUNDARY, Messages.CloseChannel);
					channel.close();
				}
			} finally {
				this.lock.unlock();
			}
		} else {
			LOG.debug(Markers.DETAIL, "race channel condition.");
		}
	}

	public void decrementProgress(String name) {
		this.channel.getPipeline().remove(name);
		this.inProgress.remove(name);
	}

	public CountDownRiakFuture newRiakFuture(String name) {
		return new CountDownRiakFuture(name, this);
	}

	public <T> RiakFuture handle(String name, Object send,
			RiakResponseHandler<T> users, MessageHandler handler) {
		CountDownRiakFuture future = newRiakFuture(name);
		ChannelHandler ch = new DefaultCompletionChannelHandler<T>(this, name,
				users, handler, future);
		return handle(name, send, users, ch, future);
	}

	public <T> RiakFuture handle(String name, Object send,
			RiakResponseHandler<T> users, ChannelHandler handler,
			RiakFuture future) {
		OperationTask cmd = new OperationTask(this.channel, send, name, handler);
		try {
			if (entry(cmd)) {
				cmd.execute();
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug(Markers.DETAIL, Messages.Queue, name);
				}
				this.waitQueue.add(cmd);
			}
			return future;
		} catch (Exception e) {
			completeOnError(name);
			throw new RiakException(e);
		} catch (Error e) {
			completeOnError(name);
			throw e;
		}
	}

	protected boolean entry(OperationTask cmd) {
		return this.inProgress.size() < 1 && this.inProgress.add(cmd.name);
	}

	protected void completeOnError(String name) {
		decrementProgress(name);
		operationComplete();
	}

	protected void invokeNext() {
		for (Iterator<OperationTask> i = this.waitQueue.iterator(); i.hasNext();) {
			OperationTask cmd = i.next();
			if (entry(cmd)) {
				i.remove();
				cmd.execute();
				break;
			}
		}
	}

	public RiakContentsResponse<_> newResponse() {
		return newResponse(_._);
	}

	public <T> RiakContentsResponse<T> newResponse(final T contents) {
		return new RiakContentsResponse<T>(contents);
	}

}
