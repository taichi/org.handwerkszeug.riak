package org.handwerkszeug.riak.transport.internal;

/**
 * @author taichi
 */
public interface MessageHandler {
	/**
	 * @param receive
	 * @return true : handle finished / false : do more handle.
	 */
	boolean handle(Object receive, CountDownRiakFuture future) throws Exception;
}