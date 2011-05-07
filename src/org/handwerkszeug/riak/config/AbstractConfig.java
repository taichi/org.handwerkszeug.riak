package org.handwerkszeug.riak.config;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.handwerkszeug.riak.Config;

/**
 * @author taichi
 */
public abstract class AbstractConfig implements Config {

	protected AbstractConfig() {
	}

	@Override
	public String getRawName() {
		return "riak";
	}

	@Override
	public String getMapReduceName() {
		return "mapred";
	}

	@Override
	public String getLuwakName() {
		return "luwak";
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
