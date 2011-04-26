package org.handwerkszeug.riak.pbc;

import static org.jboss.netty.channel.Channels.pipeline;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;

public class PbcPipelineFactory implements ChannelPipelineFactory {

	@Override
	public ChannelPipeline getPipeline() throws Exception {
		ChannelPipeline p = pipeline();
		p.addLast("riakdecoder", new RiakProtoBufDecoder());
		p.addLast("riakEncoder", new RiakProtoBufEncoder());
		return p;
	}

}
