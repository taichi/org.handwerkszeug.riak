package org.handwerkszeug.riak.op;

import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.model.Key;
import org.handwerkszeug.riak.model.Quorum;
import org.handwerkszeug.riak.model.RiakObject;

/**
 * <p>
 * not support manual sibling.<br/>
 * when you use http client and {@code get} operations,<br/>
 * client set {@code Accept : multipart/mixed} all time.<br/>
 * </p>
 * <p>
 * <h3>Siblings</h3>
 * <p>
 * When “allow_mult” is set to true in the bucket properties, concurrent updates
 * are allowed to create “sibling” objects, meaning that the object has any
 * number of different values that are related to one another by the vector
 * clock. This allows your application to use its own conflict resolution
 * technique.
 * </p>
 * <p>
 * An object with multiple values will result in a 300 Multiple Choices
 * response. If the Accept header prefers “multipart/mixed”, all siblings will
 * be returned in a single request as chunks of the “multipart/mixed” response
 * body. Otherwise, a list of “vtags” will be given in a simple text format. You
 * can request individual siblings by adding the vtag query parameter. Scroll
 * down to the “Manually requesting siblings” example for more information.
 * </p>
 * <p>
 * To resolve the conflict, store the resolved version with the X-Riak-Vclock
 * given in the response.
 * </p>
 * 
 * </p>
 * 
 * @author taichi
 */
public interface ObjectKeyOperations {

	RiakResponse<RiakObject<byte[]>> get(Key key);

	RiakResponse<RiakObject<byte[]>> get(Key key, GetOptions options);

	RiakResponse<RiakObject<byte[]>> get(Key key, GetOptions options,
			SiblingHandler handler);

	/**
	 * @see <a href="http://wiki.basho.com/Luwak.html">Luwak</a>
	 */
	void getStream(Key key, GetOptions options, InputStreamHandler handler);

	RiakResponse<_> put(Key key, RiakObject<byte[]> content);

	RiakResponse<_> put(Key key, RiakObject<byte[]> content, PutOptions options);

	/**
	 * @see <a href="http://wiki.basho.com/Luwak.html">Luwak</a>
	 */
	RiakResponse<Key> putStream(String bucket,
			RiakObject<OutputStreamHandler> content);

	RiakResponse<_> putStream(Key key, RiakObject<OutputStreamHandler> content);

	RiakResponse<_> delete(Key key);

	RiakResponse<_> delete(Key key, int quorum);

	RiakResponse<_> delete(Key key, Quorum quorum);
}
