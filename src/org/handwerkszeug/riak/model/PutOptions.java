package org.handwerkszeug.riak.model;

import java.util.Date;

/**
 * 
 * @author taichi
 */
public interface PutOptions extends StoreOptions {

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
