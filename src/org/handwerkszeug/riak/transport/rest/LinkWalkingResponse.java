package org.handwerkszeug.riak.transport.rest;

import java.util.List;

import org.handwerkszeug.riak.model.RiakObject;

/**
 * @author taichi
 */
public interface LinkWalkingResponse {

	/**
	 * @return true : walking is finished. so {@link #getResponse()} returns
	 *         empty list.
	 */
	boolean getDone();

	List<RiakObject<byte[]>> getResponse();

}
