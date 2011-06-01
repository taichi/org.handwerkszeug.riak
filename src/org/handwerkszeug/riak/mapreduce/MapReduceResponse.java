package org.handwerkszeug.riak.mapreduce;

import org.codehaus.jackson.node.ArrayNode;

/**
 * @author taichi
 */
public interface MapReduceResponse {

	/**
	 * @return phase no maybe null
	 */
	Integer getPhase();

	ArrayNode getResponse();

	boolean getDone();
}
