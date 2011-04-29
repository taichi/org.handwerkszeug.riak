package org.handwerkszeug.riak.util;

import java.util.concurrent.TimeUnit;

import org.handwerkszeug.riak.model.RiakFuture;
import org.handwerkszeug.riak.model.internal.AbstractRiakResponse;
import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;

public class NettyUtil {

	public interface MessageHandler {

		boolean handle(Object receive);
	}

	public static class FutureAdapter implements RiakFuture {
		ChannelFuture delegate;

		public FutureAdapter(ChannelFuture delegate) {
			this.delegate = delegate;
		}

		public boolean cancel() {
			return this.delegate.cancel();
		}

		public boolean await(long timeout, TimeUnit unit)
				throws InterruptedException {
			return this.delegate.await(timeout, unit);
		}

		public void awaitUninterruptibly() {
			this.delegate.awaitUninterruptibly();
		}
	}

	public static class UpstreamHandler<T> extends SimpleChannelUpstreamHandler {
		final Logger logger;
		final RiakResponseHandler<T> handler;

		public UpstreamHandler(Logger logger, RiakResponseHandler<T> handler) {
			this.logger = logger;
			this.handler = handler;
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx,
				final ExceptionEvent e) throws Exception {
			handler.handle(new AbstractRiakResponse<T>() {
				@Override
				public boolean isErrorResponse() {
					return true;
				}

				@Override
				public String getMessage() {
					return e.getCause().getMessage();
				}

				@Override
				public T getResponse() {
					return null;
				}
			});
			logger.error(e.getCause().getMessage(), e.getCause());
		}
	}
}
