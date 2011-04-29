package org.handwerkszeug.riak.mapreduce;

/**
 * 
 * @author taichi
 * @see <a
 *      href="http://wiki.basho.com/MapReduce.html#MapReduce-via-the-REST-API">MapReduce-via-the-REST-API</a>
 */
public interface MapReduceQuery {

	/**
	 * set one or more inputs
	 * 
	 * @param inputs
	 */
	void setInputs(MapReduceInputs inputs);

	/**
	 * set one or more phases.
	 * 
	 * @param mapReducePhases
	 */
	void setQueries(MapReducePhase... mapReducePhases);

	/**
	 * Map/Reduce queries have a default timeout of 60000 milliseconds (60
	 * seconds).
	 * 
	 * @param timeout
	 */
	void setTimeout(long timeout);
}
