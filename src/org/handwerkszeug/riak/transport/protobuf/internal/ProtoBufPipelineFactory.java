package org.handwerkszeug.riak.transport.protobuf.internal;

import static org.jboss.netty.channel.Channels.pipeline;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;

/**
 * @author taichi
 */
public class ProtoBufPipelineFactory implements ChannelPipelineFactory {

	@Override
	public ChannelPipeline getPipeline() throws Exception {
		ChannelPipeline p = pipeline();
		p.addLast("framedecoder", new LengthFieldBasedFrameDecoder(
				Integer.MAX_VALUE, 0, 4));

		p.addLast("riakdecoder", new RiakProtoBufDecoder());
		p.addLast("riakEncoder", new RiakProtoBufEncoder());
		return p;
	}

}
