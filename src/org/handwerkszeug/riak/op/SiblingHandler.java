package org.handwerkszeug.riak.op;

import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.model.RiakResponse;

/**
 * @author taichi
 */
public interface SiblingHandler extends RiakResponseHandler<RiakObject<byte[]>> {

	/**
	 * at the beginning of sibling.
	 */
	void begin() throws Exception;

	/**
	 * at the end of the sibling.
	 * 
	 * @return conflict resolved object.
	 */
	void end(RiakResponse response) throws Exception;
}
