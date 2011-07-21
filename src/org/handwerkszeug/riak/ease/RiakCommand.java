package org.handwerkszeug.riak.ease;

/**
 * @author taichi
 * @param <V>
 */
public interface RiakCommand<V> {

	V execute();
}
