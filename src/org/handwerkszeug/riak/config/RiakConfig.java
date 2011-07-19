package org.handwerkszeug.riak.config;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;

/**
 * @author taichi
 */
public interface RiakConfig {

	Integer getTimeout();

	InetSocketAddress getRiakAddress();

	ExecutorService getBossExecutor();

	ExecutorService getWorkerExecutor();

	ExecutorService getActionExecutor();

}
