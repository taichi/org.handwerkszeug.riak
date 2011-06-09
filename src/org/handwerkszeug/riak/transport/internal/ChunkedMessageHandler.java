package org.handwerkszeug.riak.transport.internal;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpResponse;

/**
 * @author taichi
 */
public interface ChunkedMessageHandler {
	void handle(HttpResponse response, ChannelBuffer buffer) throws Exception;
}