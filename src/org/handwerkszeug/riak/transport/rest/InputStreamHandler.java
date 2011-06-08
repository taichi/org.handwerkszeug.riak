package org.handwerkszeug.riak.transport.rest;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author taichi
 */
public interface InputStreamHandler {

	long getContentLength();

	/**
	 * close automatically.
	 */
	InputStream open() throws IOException;
}
