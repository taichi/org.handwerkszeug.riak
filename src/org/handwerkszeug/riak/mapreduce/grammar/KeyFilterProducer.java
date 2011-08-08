package org.handwerkszeug.riak.mapreduce.grammar;

import org.handwerkszeug.riak.mapreduce.MapReduceKeyFilter;

/**
 * @author taichi
 */
public interface KeyFilterProducer<T> {

	KeyFilterOrPhase<T> keyFilters(MapReduceKeyFilter primary,
			MapReduceKeyFilter... filters);
}
