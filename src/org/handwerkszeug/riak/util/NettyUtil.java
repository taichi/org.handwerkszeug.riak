package org.handwerkszeug.riak.util;

import java.util.concurrent.TimeUnit;

import org.handwerkszeug.riak.model.RiakFuture;
import org.handwerkszeug.riak.op.internal.IncomprehensibleProtocolException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

/**
 * @author taichi
 */
public class NettyUtil {

	private NettyUtil() {
	}

	public interface MessageHandler {
		/**
		 * @param receive
		 * @return true : handle finished / false : do more handle.
		 */
		boolean handle(Object receive) throws Exception;
	}

	public interface ChunkedMessageHandler {
		void handle(HttpResponse response, ChannelBuffer buffer)
				throws Exception;
	}

	public static class ChunkedMessageAggregator implements MessageHandler {

		ChannelBuffer chunkBuffer;
		HttpResponse chunkedResponse;

		final String procedure;
		final ChunkedMessageHandler handler;

		public ChunkedMessageAggregator(String procedure,
				ChunkedMessageHandler handler) {
			this.procedure = procedure;
			this.handler = handler;
		}

		@Override
		public boolean handle(Object receive) throws Exception {
			if (receive instanceof HttpResponse) {
				HttpResponse response = (HttpResponse) receive;
				if (NettyUtil.isSuccessful(response.getStatus())) {
					if (response.isChunked()) {
						this.chunkBuffer = ChannelBuffers.dynamicBuffer(2048);
						this.chunkedResponse = response;
					} else {
						this.handler.handle(response, response.getContent());
						return true;
					}
				}
			} else if (receive instanceof HttpChunk) {
				HttpChunk chunk = (HttpChunk) receive;
				boolean done = chunk.isLast();
				if (done) {
					this.handler.handle(this.chunkedResponse, this.chunkBuffer);
				} else {
					this.chunkBuffer.writeBytes(chunk.getContent());
				}
				return done;
			}
			throw new IncomprehensibleProtocolException(this.procedure);
		}
	}

	public static class FutureAdapter implements RiakFuture {
		ChannelFuture delegate;

		public FutureAdapter(ChannelFuture delegate) {
			this.delegate = delegate;
		}

		@Override
		public boolean cancel() {
			return this.delegate.cancel();
		}

		@Override
		public boolean await(long timeout, TimeUnit unit)
				throws InterruptedException {
			return this.delegate.await(timeout, unit);
		}

		@Override
		public void awaitUninterruptibly() {
			this.delegate.awaitUninterruptibly();
		}
	}

	public static boolean isError(HttpResponseStatus status) {
		int i = status.getCode();
		return 400 <= i && i <= 599;
	}

	public static boolean isSuccessful(HttpResponseStatus status) {
		int i = status.getCode();
		return 200 <= i && i <= 299;
	}
}
