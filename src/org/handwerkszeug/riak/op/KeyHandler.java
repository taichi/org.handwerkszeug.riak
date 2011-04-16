package org.handwerkszeug.riak.op;

/**
 * @author taichi
 */
public interface KeyHandler {

	/**
	 * @param key
	 * @return true : continue key iteration
	 */
	boolean handle(String key);
}
