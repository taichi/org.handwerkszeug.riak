package org.handwerkszeug.riak.op.mapreduce;

/**
 * @author taichi
 */
public interface MapReduceResponse {

	/**
	 * @return phase no maybe null
	 */
	Integer getPhase();

	byte[] getResponse();
}
