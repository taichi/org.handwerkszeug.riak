package org.handwerkszeug.riak;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;

/**
 * @author taichi
 */
public interface Config {

	String getRawName();

	String getMapReduceName();

	String getLuwakName();

	Integer getTimeout();

	InetSocketAddress getRiakAddress();

	ExecutorService getBossExecutor();

	ExecutorService getWorkerExecutor();

}
