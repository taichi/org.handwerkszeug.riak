package org.handwerkszeug.riak.mapreduce.grammar;

import java.util.concurrent.TimeUnit;

import org.handwerkszeug.riak.util.Executable;

/**
 * @author taichi
 */
public interface Timeoutable<T> {

	Executable<T> timeout(long millis);

	Executable<T> timeout(long timeout, TimeUnit unit);
}
