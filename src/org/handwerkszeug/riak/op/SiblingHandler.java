package org.handwerkszeug.riak.op;

import org.handwerkszeug.riak.model.RiakObject;

/**
 * @author taichi
 */
public interface SiblingHandler extends RiakResponseHandler<RiakObject<byte[]>> {

	/**
	 * at the beginning of sibling.
	 */
	void begin();

	/**
	 * at the end of the sibling.
	 * 
	 * @return conflict resolved object.
	 */
	void end();
}
