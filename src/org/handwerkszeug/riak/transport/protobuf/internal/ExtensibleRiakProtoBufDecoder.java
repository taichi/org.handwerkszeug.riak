package org.handwerkszeug.riak.transport.protobuf.internal;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.ChannelHandler.Sharable;

import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.MessageLite.Builder;

/**
 * @author taichi
 */
@Sharable
public class ExtensibleRiakProtoBufDecoder extends RiakProtoBufDecoder {

	protected ExtensionRegistry registry;

	public ExtensibleRiakProtoBufDecoder(ExtensionRegistry registry) {
		this.registry = registry;
	}

	@Override
	protected Builder mergeFrom(Builder builder, ChannelBuffer buffer,
			int length) throws IOException {
		if (buffer.hasArray()) {
			final int offset = buffer.readerIndex();
			return builder.mergeFrom(buffer.array(), buffer.arrayOffset()
					+ offset, length, this.registry);
		} else {
			return builder.mergeFrom(new ChannelBufferInputStream(buffer),
					this.registry);
		}
	}
}
