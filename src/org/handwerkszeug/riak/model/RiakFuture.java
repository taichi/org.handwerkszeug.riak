package org.handwerkszeug.riak.model;

import java.util.concurrent.TimeUnit;

/**
 * @author taichi
 */
public interface RiakFuture {

	void cancel();

	boolean await(long timeout, TimeUnit unit) throws InterruptedException;

	void await() throws InterruptedException;

	boolean isDone();

	boolean isSuccess();

	boolean isCanceled();

	Throwable getCause();
}
