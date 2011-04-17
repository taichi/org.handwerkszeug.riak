package org.handwerkszeug.riak.op.mapreduce;

import org.codehaus.jackson.node.ArrayNode;

/**
 * @author taichi
 * @see <a
 *      href="https://github.com/basho/riak_kv/blob/master/src/riak_kv_mapred_filters.erl">riak_kv_mapred_filters.erl</a>
 */
public class MapReduceKeyFilters {

	/*
	 * Transform functions
	 */

	/*
	 * Predicate functions
	 */

	static ArrayNode between(ArrayNode container) {
		return create("between", container);
	}

	public static MapReduceKeyFilter between(final String from, final String to) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(ArrayNode json) {
				ArrayNode node = between(json);
				node.add(from);
				node.add(to);
			}
		};
	}

	public static MapReduceKeyFilter between(final int from, final int to) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(ArrayNode json) {
				ArrayNode node = between(json);
				node.add(from);
				node.add(to);
			}
		};
	}

	public static MapReduceKeyFilter between(final long from, final long to) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(ArrayNode json) {
				ArrayNode node = between(json);
				node.add(from);
				node.add(to);
			}
		};
	}

	public static MapReduceKeyFilter between(final double from, final double to) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(ArrayNode json) {
				ArrayNode node = between(json);
				node.add(from);
				node.add(to);
			}
		};
	}

	public static MapReduceKeyFilter between(final float from, final float to) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(ArrayNode json) {
				ArrayNode node = between(json);
				node.add(from);
				node.add(to);
			}
		};
	}

	public static MapReduceKeyFilter and(MapReduceKeyFilter left,
			MapReduceKeyFilter right) {
		return null;
	}

	protected static ArrayNode create(String name, ArrayNode container) {
		ArrayNode node = container.addArray();
		node.add(name);
		return node;
	}
}
