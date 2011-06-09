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
		return false;
	}
}