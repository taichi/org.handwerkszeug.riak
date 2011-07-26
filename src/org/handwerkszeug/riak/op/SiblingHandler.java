package org.handwerkszeug.riak.op;

import org.handwerkszeug.riak.model.RiakObject;

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
	 */
	void end() throws Exception;
}
