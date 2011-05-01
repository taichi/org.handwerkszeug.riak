package org.handwerkszeug.riak.http;

import java.io.IOException;
import java.io.InputStream;

import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.model.RiakObject;

/**
 * @author taichi
 */
public interface InputStreamHandler {

	void handle(RiakContentsResponse<RiakObject<InputStream>> response)
			throws IOException;
}
