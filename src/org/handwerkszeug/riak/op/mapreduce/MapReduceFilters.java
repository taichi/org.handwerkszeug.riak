package org.handwerkszeug.riak.op.mapreduce;

import org.codehaus.jackson.node.ObjectNode;

/**
 * @author taichi
 * @see <a
 *      href="https://github.com/basho/riak_kv/blob/master/src/riak_kv_mapred_filters.erl">riak_kv_mapred_filters.erl</a>
 */
public class MapReduceFilters {

	public interface MapReduceFilter {

		ObjectNode toJson();
	}

	public static MapReduceFilter between(String from, String to) {
		return null;
	}

	public static MapReduceFilter and(MapReduceFilter left,
			MapReduceFilter right) {
		return null;
	}
}
