package org.handwerkszeug.riak.mapreduce.grammar;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.handwerkszeug.riak.mapreduce.MapReduceInput;

/**
 * @author taichi
 */
public interface InputsProducer<T> {

	KeyFilterOrPhase<T> inputs(String bucket);

	PhaseProducer<T> inputs(String bucket, String key);

	PhaseProducer<T> inputs(String bucket, String key, Object keyData);

	PhaseProducer<T> inputs(String bucket, String key, String keyData);

	PhaseProducer<T> inputs(String bucket, String key, int keyData);

	PhaseProducer<T> inputs(String bucket, String key, long keyData);

	PhaseProducer<T> inputs(String bucket, String key, double keyData);

	PhaseProducer<T> inputs(String bucket, String key, float keyData);

	PhaseProducer<T> inputs(String bucket, String key, BigInteger keyData);

	PhaseProducer<T> inputs(String bucket, String key, BigDecimal keyData);

	PhaseProducer<T> inputs(MapReduceInput primary, MapReduceInput... inputs);

	/**
	 * @see <a
	 *      href="http://wiki.basho.com/Riak-Search---Querying.html#Querying-Integrated-with-Map-Reduce">Querying
	 *      Integrated with Map/Reduce </a>
	 * @see <a
	 *      href="https://github.com/basho/riak_search/blob/master/apps/riak_search/src/riak_search.erl">riak_search.erl</a>
	 */
	PhaseProducer<T> search(String index, String query);

}
