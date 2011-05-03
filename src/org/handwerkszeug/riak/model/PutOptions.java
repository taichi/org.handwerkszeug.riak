package org.handwerkszeug.riak.model;

import java.util.Date;

/**
 * 
 * @author taichi
 */
public interface PutOptions {

	/**
	 * opaque vector clock provided by an earlier RpbGetResp message. Omit if
	 * this is a new key or you deliberately want to create a sibling
	 */
	String getVectorClock();

	/**
	 * how many replicas need to agree when retrieving an existing object before
	 * the write (default is defined by the bucket|#Set bucket properties)
	 */
	Quorum getReadQuorum();

	/**
	 * how many replicas to write to before returning a successful response;
	 * possible values include ‘default’, ‘one’, ‘quorum’, ‘all’, or any integer
	 * <= N (default is defined per the bucket)
	 */
	Quorum getWriteQuorum();

	/**
	 * how many replicas to commit to durable storage before returning a
	 * successful response; possible values include ‘default’, ‘one’, ‘quorum’,
	 * ‘all’, or any integer <= N (default is defined per the bucket)
	 */
	Quorum getDurableWriteQuorum();

	/**
	 * whether to return the contents of the stored object. Defaults to false.
	 */
	boolean getReturnBody();

	/**
	 * @return etag
	 */
	String getIfNoneMatch();

	/**
	 * @return etag
	 */
	String getIfMatch();

	Date getIfModifiedSince();

	Date getIfUnmodifiedSince();
}
