package org.handwerkszeug.riak.op;

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
	 * how many replicas to write to before returning a successful response;
	 * possible values include ‘default’, ‘one’, ‘quorum’, ‘all’, or any integer
	 * <= N (default is defined per the bucket)
	 */
	int getWriteQuorum();

	/**
	 * how many replicas to commit to durable storage before returning a
	 * successful response; possible values include ‘default’, ‘one’, ‘quorum’,
	 * ‘all’, or any integer <= N (default is defined per the bucket)
	 */
	int getDurableWriteQuorum();

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
