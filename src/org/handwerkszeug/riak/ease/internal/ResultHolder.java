package org.handwerkszeug.riak.ease.internal;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.handwerkszeug.riak.RiakException;

/**
 * @author taichi
 * @param <T>
 */
public class ResultHolder<T> {

	final CountDownLatch latch;
	T result;

	AtomicBoolean failed = new AtomicBoolean(false);
	String message;

	public ResultHolder() {
		this.latch = new CountDownLatch(1);
	}

	public void setResult(T result) {
		this.result = result;
		this.latch.countDown();
	}

	public T getResult() {
		try {
			this.latch.await();
			if (this.failed.get()) {
				throw new RiakException(this.message);
			}
			return this.result;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RiakException(e);
		}
	}

	public T getResult(long timeout, TimeUnit unit) {
		try {
			this.latch.await(timeout, unit);
			if (this.failed.get()) {
				throw new RiakException(this.message);
			}
			return this.result;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RiakException(e);
		}
	}

	public void fail(String message) {
		this.failed.compareAndSet(false, true);
		this.message = message;
		this.latch.countDown();
	}
}
