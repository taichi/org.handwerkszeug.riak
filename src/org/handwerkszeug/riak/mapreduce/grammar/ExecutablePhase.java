package org.handwerkszeug.riak.mapreduce.grammar;

/**
 * @author taichi
 */
public interface ExecutablePhase<T> extends Executable<T>, PhaseProducer<T>,
		Timeoutable<T> {
}
