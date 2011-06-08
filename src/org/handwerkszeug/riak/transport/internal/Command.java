package org.handwerkszeug.riak.transport.internal;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;

/**
 * @author taichi
 */
class Command {
	Channel channel;
	Object message;
	String name;
	ChannelHandler handler;

	public Command(Channel channel, Object message, String name,
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
		this.channel.write(this.message);
	}
}