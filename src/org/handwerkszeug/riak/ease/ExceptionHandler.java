package org.handwerkszeug.riak.ease;

import org.handwerkszeug.riak.model.RiakResponse;

/**
 * @author taichi
 */
public interface ExceptionHandler {
	void handle(RiakCommand<?> cmd, RiakResponse response);
}
