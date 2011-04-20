package org.handwerkszeug.riak.op;

import java.util.concurrent.TimeUnit;

/**
 * @author taichi
 */
public interface RiakFuture {

	void cancel();

	boolean await(long timeout, TimeUnit unit);
}
