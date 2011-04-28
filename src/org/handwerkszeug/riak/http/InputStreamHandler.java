package org.handwerkszeug.riak.http;

import java.io.IOException;
import java.io.InputStream;

import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.model.RiakResponse;

/**
 * @author taichi
 */
public interface InputStreamHandler {

	void handle(RiakResponse<RiakObject<InputStream>> response)
			throws IOException;
}
