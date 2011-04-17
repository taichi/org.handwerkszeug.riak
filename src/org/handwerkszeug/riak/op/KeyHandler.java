package org.handwerkszeug.riak.op;

import org.handwerkszeug.riak._;

/**
 * @author taichi
 */
public interface KeyHandler {

	/**
	 * @param key
	 * @return true : continue key iteration
	 */
	boolean handleKey(RiakResponse<Iterable<String>> current);

	void handleDone(RiakResponse<_> response);
}
