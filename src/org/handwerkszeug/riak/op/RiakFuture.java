package org.handwerkszeug.riak.op;

import java.util.concurrent.TimeUnit;

/**
 * @author taichi
 */
public interface RiakFuture {

	public boolean cancel();

	public boolean await(long timeout, TimeUnit unit)
			throws InterruptedException;
}
