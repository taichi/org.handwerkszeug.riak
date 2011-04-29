package org.handwerkszeug.riak.op;

import java.util.List;

import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.model.RiakResponse;

/**
 * @author taichi
 */
public interface KeyHandler extends RiakResponseHandler<_> {

	void handleKeys(RiakResponse<List<String>> response, boolean done);
}
