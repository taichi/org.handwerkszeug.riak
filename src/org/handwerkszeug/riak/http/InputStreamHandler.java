package org.handwerkszeug.riak.http;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author taichi
 */
public interface InputStreamHandler {

	InputStream open() throws IOException;

	void close(InputStream out) throws IOException;
}
