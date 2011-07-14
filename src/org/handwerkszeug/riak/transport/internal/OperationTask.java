package org.handwerkszeug.riak.transport.internal;

import org.handwerkszeug.riak.Markers;
import org.handwerkszeug.riak.nls.Messages;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author taichi
 */
class OperationTask {

	static final Logger LOG = LoggerFactory.getLogger(OperationTask.class);

	Channel channel;
	Object message;
	String name;
	ChannelHandler handler;

	public OperationTask(Channel channel, Object message, String name,
			ChannelHandler handler) {
		super();
		this.channel = channel;
		this.message = message;
		this.name = name;
		this.handler = handler;
	}

	public void execute() {
		ChannelPipeline pipeline = this.channel.getPipeline();
		pipeline.addLast(this.name, this.handler);
		if (LOG.isDebugEnabled()) {
			LOG.debug(Markers.BOUNDARY, Messages.SendTo, this.name,
					this.channel.getRemoteAddress());
		}
		this.channel.write(this.message);
	}
}