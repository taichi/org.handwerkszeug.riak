package org.handwerkszeug.riak.op;

import java.io.IOException;
import java.io.InputStream;

import org.handwerkszeug.riak.model.RiakObject;

/**
 * @author taichi
 */
public interface InputStreamHandler {

	void handle(RiakResponse<RiakObject<InputStream>> response)
			throws IOException;
}
