package org.handwerkszeug.riak.mapreduce;

import java.util.Collection;

/**
 * 
 * @author taichi
 * @see <a
 *      href="http://wiki.basho.com/MapReduce.html#MapReduce-via-the-REST-API">MapReduce-via-the-REST-API</a>
 */
public interface MapReduceQuery {

	/**
	 * all of the keys in that bucket as inputs
	 * 
	 * @param bucket
	 */
	void setInputs(String bucket);

	void setInputs(Collection<MapReduceInput> inputs);

	void setInputs(MapReduceSearchInput search);

	void setQueries(Collection<MapReducePhase> mapReducePhases);

	/**
	 * Map/Reduce queries have a default timeout of 60000 milliseconds (60
	 * seconds).
	 * 
	 * @param timeout
	 */
	void setTimeout(long timeout);

	void clear();
}
