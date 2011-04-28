package org.handwerkszeug.riak.model;

import java.util.Date;


/**
 * not support manual siblings.
 * 
 * @author taichi
 */
public interface GetOptions {

	/**
	 * how many replicas need to agree when retrieving the object; possible
	 * values include ‘default’, ‘one’, ‘quorum’, ‘all’, or any integer <= N
	 * (default is defined per the bucket)
	 */
	Quorum getReadQuorum();

	/**
	 * when accessing an object with siblings, which sibling to retrieve. Scroll
	 * down to the “Manually requesting siblings” example for more information.
	 * 
	 */
	// String getVtag();

	/**
	 * @return etag
	 */
	String getIfNoneMatch();

	/**
	 * @return etag
	 */
	String getIfMatch();

	Date getIfModifiedSince();

}
