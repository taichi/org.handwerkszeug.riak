package org.handwerkszeug.riak.http;

import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.op.GetOptions;
import org.handwerkszeug.riak.op.RiakFuture;
import org.handwerkszeug.riak.op.RiakResponseHandler;

/**
 * @see <a href="http://wiki.basho.com/Luwak.html">Luwak</a>
 * @author taichi
 */
public interface LuwakSupport {

	RiakFuture getStream(Location key, GetOptions options,
			InputStreamHandler handler);

	RiakFuture putStream(String bucket,
			RiakObject<OutputStreamHandler> content,
			RiakResponseHandler<Location> handler);

	RiakFuture putStream(RiakObject<OutputStreamHandler> content,
			RiakResponseHandler<_> handler);
}
