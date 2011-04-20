package org.handwerkszeug.riak.http;

import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.op.GetOptions;
import org.handwerkszeug.riak.op.RiakResponseHandler;

/**
 * @see <a href="http://wiki.basho.com/Luwak.html">Luwak</a>
 * @author taichi
 */
public interface LuwakSupport {

	void getStream(Location key, GetOptions options, InputStreamHandler handler);

	void putStream(String bucket, RiakObject<OutputStreamHandler> content,
			RiakResponseHandler<Location> handler);

	void putStream(RiakObject<OutputStreamHandler> content,
			RiakResponseHandler<_> handler);
}
