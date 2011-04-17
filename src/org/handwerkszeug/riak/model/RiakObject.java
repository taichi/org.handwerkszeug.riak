package org.handwerkszeug.riak.model;

/**
 * 
 * @author taichi
 * @see <a
 *      href="https://github.com/basho/riak_kv/blob/master/src/riak_object.erl">riak_object.erl</a>
 */
public interface RiakObject<T> {

	T getContent();

	// TODO ???
}
