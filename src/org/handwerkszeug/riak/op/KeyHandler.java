package org.handwerkszeug.riak.op;

import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.model.RiakResponse;

/**
 * @author taichi
 */
public interface KeyHandler {

	/**
	 * @param key
	 * @return true : continue key iteration
	 */
	boolean handle(RiakResponse<Iterable<String>> current);

	void handleDone(RiakResponse<_> response);
}
