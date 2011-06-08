package org.handwerkszeug.riak.transport.rest;

import org.handwerkszeug.riak.model.RiakFuture;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.op.RiakResponseHandler;

/**
 * @author taichi
 */
public interface SearchSupport {

	/**
	 * @see <a href="http://wiki.basho.com/Riak-Search---Querying.html">Riak
	 *      Search Querying</a>
	 * @see <a
	 *      href="http://lucene.apache.org/java/2_4_0/queryparsersyntax.html">Apache
	 *      Lucene - Query Parser Syntax</a>
	 */
	RiakFuture search(String query,
			RiakResponseHandler<RiakObject<byte[]>> handler);
	// TODO temporally define. you need think about this.
}
