package org.handwerkszeug.riak.util;

import java.util.concurrent.TimeUnit;

import org.handwerkszeug.riak.model.RiakFuture;
import org.jboss.netty.channel.ChannelFuture;

/**
 * @author taichi
 */
public class NettyUtil {

	public interface MessageHandler {
		/**
		 * @param receive
		 * @return true : handle finished / false : do more handle.
		 */
		boolean handle(Object receive);
	}

	public static class FutureAdapter implements RiakFuture {
		ChannelFuture delegate;

		public FutureAdapter(ChannelFuture delegate) {
			this.delegate = delegate;
		}

		public boolean cancel() {
			return this.delegate.cancel();
		}

		public boolean await(long timeout, TimeUnit unit)
				throws InterruptedException {
			return this.delegate.await(timeout, unit);
		}

		public void awaitUninterruptibly() {
			this.delegate.awaitUninterruptibly();
		}
	}

}
