package org.handwerkszeug.riak.config;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * @author taichi
 */
public abstract class AbstractConfig implements Config {

	final InetSocketAddress address;

	protected AbstractConfig(InetSocketAddress address) {
		this.address = address;
	}

	@Override
	public InetSocketAddress getRiakAddress() {
		return this.address;
	}

	@Override
	public Integer getTimeout() {
		return 60000;
	}

	@Override
	public ExecutorService getBossExecutor() {
		return Executors.newCachedThreadPool();
	}

	@Override
	public ExecutorService getWorkerExecutor() {
		return Executors.newCachedThreadPool();
	}

}
