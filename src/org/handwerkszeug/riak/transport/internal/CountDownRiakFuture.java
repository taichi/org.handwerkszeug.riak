package org.handwerkszeug.riak.transport.internal;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.handwerkszeug.riak.Markers;
import org.handwerkszeug.riak.model.RiakFuture;
import org.jboss.netty.channel.ChannelPipeline;
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
	ChannelPipeline pipeline;
	boolean success;
	boolean canceled;
	Throwable cause;

	public CountDownRiakFuture(String name, ChannelPipeline pipeline) {
		this.latch = new CountDownLatch(1);
		this.name = name;
		this.pipeline = pipeline;
	}

	public void finished() {
		LOG.debug(Markers.DETAIL, "finished {}", this.name); // TODO message
		this.pipeline.remove(this.name);
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
		return this.success || this.canceled || this.cause != null;
	}

	@Override
	public boolean isSuccess() {
		this.latch.countDown();
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

	public void setName(String name) {
		this.name = name;
	}
}