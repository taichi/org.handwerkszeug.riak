package org.handwerkszeug.riak.http;

import org.handwerkszeug.riak.model.GetOptions;
import org.handwerkszeug.riak.model.RiakFuture;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.op.RiakResponseHandler;

/**
 * @author taichi
 * @see <a href="http://wiki.basho.com/Luwak.html">Luwak</a>
 */
public interface LuwakSupport {

	RiakFuture getStream(String key, GetOptions options,
			InputStreamHandler handler);

	RiakFuture putStream(RiakObject<OutputStreamHandler> content,
			RiakResponseHandler<String> handler);

}
