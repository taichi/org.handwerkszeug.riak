package org.handwerkszeug.riak.model;

import java.util.concurrent.TimeUnit;

/**
 * @author taichi
 */
public interface RiakFuture {

	boolean cancel();

	boolean await(long timeout, TimeUnit unit) throws InterruptedException;

	void awaitUninterruptibly();
}
