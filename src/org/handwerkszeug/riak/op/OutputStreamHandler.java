package org.handwerkszeug.riak.op;

import java.io.IOException;
import java.io.OutputStream;

public interface OutputStreamHandler {

	OutputStream open() throws IOException;

	void close(OutputStream out) throws IOException;
}
