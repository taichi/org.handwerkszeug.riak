package org.handwerkszeug.riak.op;

import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.model.GetOptions;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.PutOptions;
import org.handwerkszeug.riak.model.Quorum;
import org.handwerkszeug.riak.model.RiakFuture;
import org.handwerkszeug.riak.model.RiakObject;

/**
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

	RiakFuture get(Location location,
			RiakResponseHandler<RiakObject<byte[]>> handler);

	RiakFuture get(Location location, GetOptions options,
			RiakResponseHandler<RiakObject<byte[]>> handler);

	RiakFuture get(Location location, GetOptions options, SiblingHandler handler);

	/**
	 * Stores a new object in a bucket with a random Riak-assigned key.<br/>
	 * set Location key as empty string.
	 */
	RiakFuture post(RiakObject<byte[]> content,
			RiakResponseHandler<RiakObject<byte[]>> handler);

	/**
	 * Stores a new object in a bucket with a random Riak-assigned key.<br/>
	 * set Location key as empty string.
	 */
	RiakFuture post(RiakObject<byte[]> content, PutOptions options,
			RiakResponseHandler<RiakObject<byte[]>> handler);

	RiakFuture put(RiakObject<byte[]> content, RiakResponseHandler<_> handler);

	RiakFuture put(RiakObject<byte[]> content, PutOptions options,
			SiblingHandler handler);

	RiakFuture delete(Location location, RiakResponseHandler<_> handler);

	/**
	 * @param key
	 * @param quorum
	 *            quorum for both operations (get and put) involved in deleting
	 *            an object. (default is set at the bucket level)
	 * @return
	 */
	RiakFuture delete(Location location, Quorum readWrite,
			RiakResponseHandler<_> handler);
}
