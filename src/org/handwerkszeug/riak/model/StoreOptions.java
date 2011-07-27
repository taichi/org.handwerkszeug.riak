package org.handwerkszeug.riak.model;

/**
 * @author taichi
 */
public interface StoreOptions {

	/**
	 * how many replicas to write to before returning a successful response
	 * (default is defined by the bucket level)
	 */
	Quorum getWriteQuorum();

	/**
	 * how many replicas to commit to durable storage before returning a
	 * successful response (default is defined at the bucket level)
	 */
	Quorum getDurableWriteQuorum();

	/**
	 * whether to return the contents of the stored object. Defaults to false.
	 */
	boolean getReturnBody();

}
