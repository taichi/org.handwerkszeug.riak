package org.handwerkszeug.riak.transport.internal;

import org.handwerkszeug.riak.util.NettyUtil;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;

/**
 * @author taichi
 */
public class ChunkedMessageAggregator implements MessageHandler {

	ChannelBuffer chunkedBuffer;
	HttpResponse chunkedResponse;

	final String procedure;
	final MessageHandler handler;

	public ChunkedMessageAggregator(String procedure, MessageHandler handler) {
		this.procedure = procedure;
		this.handler = handler;
	}

	@Override
	public boolean handle(Object receive, CountDownRiakFuture future)
			throws Exception {
		if (receive instanceof HttpResponse) {
			HttpResponse response = (HttpResponse) receive;
			if (NettyUtil.isSuccessful(response.getStatus())) {
				if (response.isChunked()) {
					this.chunkedBuffer = response.getContent();
					this.chunkedResponse = response;
				} else {
					this.handler.handle(response, future);
					return true;
				}
			}
		} else if (receive instanceof HttpChunk) {
			HttpChunk chunk = (HttpChunk) receive;
			boolean done = chunk.isLast();
			if (done) {
				this.chunkedResponse.setChunked(false);
				this.chunkedResponse.setContent(this.chunkedBuffer);
				this.handler.handle(this.chunkedResponse, future);
			} else {
				this.chunkedBuffer = ChannelBuffers.wrappedBuffer(
						this.chunkedBuffer, chunk.getContent());
			}
			return done;
		}
		return false;
	}
}