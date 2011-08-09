package org.handwerkszeug.riak.mapreduce.grammar;

import org.handwerkszeug.riak.util.Executable;

/**
 * @author taichi
 */
public interface ExecutablePhase<T> extends Executable<T>, PhaseProducer<T>,
		Timeoutable<T> {
}
