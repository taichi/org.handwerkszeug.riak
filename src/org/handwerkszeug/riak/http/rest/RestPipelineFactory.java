package org.handwerkszeug.riak.http.rest;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.http.HttpClientCodec;
import org.jboss.netty.handler.codec.http.HttpContentDecompressor;
import org.jboss.netty.handler.codec.http.MultipartResponseDecoder;

/**
 * @author taichi
 */
public class RestPipelineFactory implements ChannelPipelineFactory {

	@Override
	public ChannelPipeline getPipeline() throws Exception {
		ChannelPipeline pipeline = Channels.pipeline();

		pipeline.addLast("codec", new HttpClientCodec());
		pipeline.addLast("inflater", new HttpContentDecompressor());
		pipeline.addLast("multipart/mixed",
				new MultipartResponseDecoder());

		return pipeline;
	}

}
