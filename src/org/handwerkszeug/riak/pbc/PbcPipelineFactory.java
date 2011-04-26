package org.handwerkszeug.riak.pbc;

import static org.jboss.netty.channel.Channels.pipeline;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;

/**
 * @author taichi
 */
public class PbcPipelineFactory implements ChannelPipelineFactory {

	@Override
	public ChannelPipeline getPipeline() throws Exception {
		ChannelPipeline p = pipeline();
		p.addLast("riakdecoder", new RiakProtoBufDecoder());
		p.addLast("riakEncoder", new RiakProtoBufEncoder());
		return p;
	}

}
