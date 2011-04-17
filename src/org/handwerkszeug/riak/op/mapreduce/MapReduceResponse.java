package org.handwerkszeug.riak.op.mapreduce;

import org.codehaus.jackson.JsonNode;

/**
 * @author taichi
 */
public interface MapReduceResponse {

	/**
	 * @return phase no maybe null
	 */
	Integer getPhase();

	JsonNode getResponse();
}
