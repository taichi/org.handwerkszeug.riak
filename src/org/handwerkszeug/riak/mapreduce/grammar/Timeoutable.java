package org.handwerkszeug.riak.mapreduce.grammar;

import java.util.concurrent.TimeUnit;

/**
 * @author taichi
 */
public interface Timeoutable<T> {

	Executable<T> timeout(long millis);

	Executable<T> timeout(long timeout, TimeUnit unit);
}
