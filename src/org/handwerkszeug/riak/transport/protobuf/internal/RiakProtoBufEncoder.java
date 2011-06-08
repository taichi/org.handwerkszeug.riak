package org.handwerkszeug.riak.transport.protobuf.internal;

import java.io.OutputStream;

import org.handwerkszeug.riak.nls.Messages;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

import com.google.protobuf.MessageLite;

/**
 * @author taichi
 * @see <a href="http://wiki.basho.com/PBC-API.html">PBC-API</a>
 */
@Sharable
public class RiakProtoBufEncoder extends OneToOneEncoder {

	static final int LEN = 4;
	static final int MC = 1;

	@Override
	protected Object encode(ChannelHandlerContext ctx, Channel channel,
			Object msg) throws Exception {
		if (msg instanceof MessageLite) {
			MessageLite ml = (MessageLite) msg;
			int length = ml.getSerializedSize();
			if (length < 0) {
				throw new IllegalStateException(String.format(
						Messages.UnsupportedContentLength, length));
			}
			MessageCodes mc = MessageCodes.valueOf(msg.getClass());
			if (mc != null) {
				length += 1;
				ChannelBuffer buffer = channel.getConfig().getBufferFactory()
						.getBuffer(LEN + MC + length);
				buffer.writeInt(length);
				buffer.writeByte(mc.getValue());
				OutputStream out = new ChannelBufferOutputStream(buffer);
				ml.writeTo(out);
				return buffer;
			}
		}
		if (msg instanceof MessageCodes) {
			MessageCodes mc = (MessageCodes) msg;
			ChannelBuffer buffer = channel.getConfig().getBufferFactory()
					.getBuffer(LEN + MC);
			buffer.writeInt(1);
			buffer.writeByte(mc.getValue());
			return buffer;
		}
		return msg;
	}
}
