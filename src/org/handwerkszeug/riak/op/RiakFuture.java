package org.handwerkszeug.riak.op;

import java.util.concurrent.TimeUnit;

import org.jboss.netty.channel.ChannelFuture;

/**
 * @author taichi
 */
public class RiakFuture {

	ChannelFuture delegate;

	public RiakFuture(ChannelFuture delegate) {
		this.delegate = delegate;
	}

	public boolean cancel() {
		return this.delegate.cancel();
	}

	public boolean await(long timeout, TimeUnit unit)
			throws InterruptedException {
		return this.delegate.await(timeout, unit);
	}
}
