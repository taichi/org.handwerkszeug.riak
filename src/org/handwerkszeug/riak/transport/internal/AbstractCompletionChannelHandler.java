package org.handwerkszeug.riak.transport.internal;

import org.handwerkszeug.riak.Markers;
import org.handwerkszeug.riak.model.AbstractRiakResponse;
import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author taichi
 * @param <T>
 */
public abstract class AbstractCompletionChannelHandler<T> extends
		SimpleChannelHandler {

	static final Logger LOG = LoggerFactory
			.getLogger(AbstractCompletionChannelHandler.class);

	final CompletionSupport support;
	final String name;
	final RiakResponseHandler<T> users;
	final CountDownRiakFuture future;

	public AbstractCompletionChannelHandler(CompletionSupport support,
			String name, RiakResponseHandler<T> users,
			CountDownRiakFuture future) {
		this.support = support;
		this.name = name;
		this.users = users;
		this.future = future;
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx,
			final ExceptionEvent e) throws Exception {
		LOG.error(Markers.BOUNDARY, e.getCause().getMessage(), e.getCause());
		this.users.onError(new AbstractRiakResponse() {
			@Override
			public String getMessage() {
				return e.getCause().getMessage();
			}
		});
		this.future.setFailure(e.getCause());
		this.support.invokeNext();
	}

	protected void setFailure(Throwable ex) {
		LOG.error(Markers.DETAIL, ex.getMessage(), ex);
		this.future.setFailure(ex);
		this.support.invokeNext();
	}

}