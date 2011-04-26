package org.handwerkszeug.riak.pbc;

import java.io.IOException;

import org.handwerkszeug.riak.nls.Messages;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;

import com.google.protobuf.MessageLite;

/**
 * @author taichi
 * @see <a href="http://wiki.basho.com/PBC-API.html">PBC-API</a>
 */
@Sharable
public class RiakProtoBufDecoder extends OneToOneDecoder {

	public RiakProtoBufDecoder() {
	}

	@Override
	protected Object decode(ChannelHandlerContext ctx, Channel channel,
			Object msg) throws Exception {
		if (msg instanceof ChannelBuffer) {
			ChannelBuffer buffer = (ChannelBuffer) msg;
			int length = buffer.readInt();
			if (length < 1) {
				throw new IllegalStateException(String.format(
						Messages.UnsupportedContentLength, length));
			}
			byte code = buffer.readByte();
			MessageCodes mc = MessageCodes.valueOf(code);
			MessageLite proto = mc.getPrototype();
			if (proto != null) {
				MessageLite.Builder builder = mergeFrom(
						proto.newBuilderForType(), buffer);
				MessageLite newone = builder.build();
				return newone;
			} else {
				return mc;
			}
		}
		return msg;
	}

	protected MessageLite.Builder mergeFrom(MessageLite.Builder builder,
			ChannelBuffer buffer) throws IOException {
		if (buffer.hasArray()) {
			final int offset = buffer.readerIndex();
			return builder.mergeFrom(buffer.array(), buffer.arrayOffset()
					+ offset, buffer.readableBytes());
		} else {
			return builder.mergeFrom(new ChannelBufferInputStream(buffer));
		}
	}

}
