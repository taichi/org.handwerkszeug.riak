package org.handwerkszeug.riak.mapreduce.grammar;

/**
 * @author taichi
 */
public interface KeyFilterOrPhase<T> extends PhaseProducer<T>,
		KeyFilterProducer<T> {

}
