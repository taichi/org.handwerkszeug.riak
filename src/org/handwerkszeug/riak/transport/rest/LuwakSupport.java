package org.handwerkszeug.riak.transport.rest;

import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.model.Range;
import org.handwerkszeug.riak.model.RiakFuture;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.op.RiakResponseHandler;

/**
 * @author taichi
 * @see <a href="http://wiki.basho.com/Luwak.html">Luwak</a>
 * @see <a
 *      href="https://github.com/basho/luwak/blob/master/src/luwak_wm_file.erl">luwak_wm_file.erl</a>
 */
public interface LuwakSupport {

	/**
	 * Reads a Luwak file
	 */
	RiakFuture getStream(String key, StreamResponseHandler handler);

	/**
	 * Reads a Luwak file by defined Range.
	 */
	RiakFuture getStream(String key, Range range, StreamResponseHandler handler);

	/**
	 * Stores a new file with a random Luwak-assigned key.
	 */
	RiakFuture postStream(RiakObject<InputStreamHandler> content,
			RiakResponseHandler<String> handler);

	/**
	 * Stores a file with an existing or user-defined key.
	 */
	RiakFuture putStream(RiakObject<InputStreamHandler> content,
			RiakResponseHandler<_> handler);

	/**
	 * Deletes a file.
	 */
	RiakFuture delete(String key, RiakResponseHandler<_> handler);

}
