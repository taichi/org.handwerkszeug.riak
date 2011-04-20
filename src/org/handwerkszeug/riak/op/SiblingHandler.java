package org.handwerkszeug.riak.op;

import org.handwerkszeug.riak.model.RiakObject;

/**
 * @author taichi
 */
public interface SiblingHandler {

	/**
	 * at the beginning of sibling.
	 */
	void begin();

	/**
	 * @param sibling
	 * @return true : continue handling.
	 */
	boolean handle(RiakObject<byte[]> sibling);

	/**
	 * at the end of the sibling.
	 * 
	 * @return conflict resolved object.
	 */
	void end();
}
