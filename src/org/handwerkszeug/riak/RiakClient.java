package org.handwerkszeug.riak;

/**
 * @author taichi
 */
public interface RiakClient {

	<T> T execute(RiakAction<T> action);

	void dispose();
}
