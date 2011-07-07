package org.handwerkszeug.riak.transport.internal;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.handwerkszeug.riak.Markers;
import org.handwerkszeug.riak.model.RiakFuture;
import org.handwerkszeug.riak.nls.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author taichi
 */
public class CountDownRiakFuture implements RiakFuture {

	static final Logger LOG = LoggerFactory
			.getLogger(CountDownRiakFuture.class);

	CountDownLatch latch;
	String name;
	CompletionSupport support;
	boolean success;
	boolean canceled;
	boolean failure;
	Throwable cause;

	public CountDownRiakFuture(String name, CompletionSupport support) {
		this.latch = new CountDownLatch(1);
		this.name = name;
		this.support = support;
	}

	public void finished() {
		LOG.debug(Markers.LIFECYCLE, Messages.Finished, this.name);
		this.support.decrementProgress(this.name);
		this.latch.countDown();
	}

	@Override
	public void cancel() {
		this.canceled = true;
		finished();
	}

	@Override
	public boolean await(long timeout, TimeUnit unit)
			throws InterruptedException {
		return this.latch.await(timeout, unit);
	}

	@Override
	public void await() throws InterruptedException {
		this.latch.await();
	}

	@Override
	public boolean isDone() {
		return this.latch.getCount() < 1;
	}

	@Override
	public boolean isSuccess() {
		return this.success;
	}

	public void setSuccess() {
		this.success = true;
		finished();
	}

	@Override
	public boolean isCanceled() {
		return this.canceled;
	}

	@Override
	public Throwable getCause() {
		return this.cause;
	}

	public void setFailure(Throwable cause) {
		this.cause = cause;
		finished();
	}

	public void setFailure() {
		this.failure = true;
		finished();
	}

	public void setName(String name) {
		this.name = name;
	}
}