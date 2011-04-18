package org.handwerkszeug.riak.op.mapreduce;

import org.codehaus.jackson.node.ArrayNode;

/**
 * {@link MapReduceKeyFilter} Factory.<br/>
 * there are shortcut methods. so you may not use there.
 * 
 * @author taichi
 * @see <a
 *      href="https://github.com/basho/riak_kv/blob/master/src/riak_kv_mapred_filters.erl">riak_kv_mapred_filters.erl</a>
 */
public class MapReduceKeyFilters {

	/**
	 * Transform functions
	 */
	public static class Transform {

		/**
		 * Turns an integer (previously extracted with string_to_int), into a
		 * string.
		 */
		public static MapReduceKeyFilter intToString() {
			return InternalFunctions.intToString;
		}

		/**
		 * Turns a string into an integer.
		 */
		public static MapReduceKeyFilter stringToInt() {
			return InternalFunctions.stringToInt;
		}

		/**
		 * Turns a floating point number (previously extracted with
		 * string_to_float), into a string.
		 */
		public static MapReduceKeyFilter floatToString() {
			return InternalFunctions.floatToString;
		}

		/**
		 * Turns a string into a floating point number.
		 */
		public static MapReduceKeyFilter stringToFloat() {
			return InternalFunctions.stringToFloat;
		}

		/**
		 * Changes all letters to uppercase.
		 */
		public static MapReduceKeyFilter toUpper() {
			return InternalFunctions.toUpper;
		}

		/**
		 * Changes all letters to lowercase.
		 */
		public static MapReduceKeyFilter toLower() {
			return InternalFunctions.toLower;
		}

		/**
		 * Splits the input on the string given as the first argument and
		 * returns the nth token specified by the second argument.
		 * 
		 * @param delimiter
		 * @param nth
		 */
		public static MapReduceKeyFilter tokenize(final String delimiter,
				final int nth) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.create("tokenize", json);
					node.add(delimiter);
					node.add(nth);
				}
			};
		}

		/**
		 * URL-decodes the string.
		 */
		public static MapReduceKeyFilter urldecode() {
			return InternalFunctions.urldecode;
		}
	}

	/**
	 * Predicate functions
	 */
	public static class Predicates {

		public static MapReduceKeyFilter between(final String from,
				final String to) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.between(json);
					node.add(from);
					node.add(to);
				}
			};
		}

		public static MapReduceKeyFilter between(final String from,
				final String to, final boolean inclusive) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.between(json);
					node.add(from);
					node.add(to);
					node.add(inclusive);
				}
			};
		}

		public static MapReduceKeyFilter between(final int from, final int to) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.between(json);
					node.add(from);
					node.add(to);
				}
			};
		}

		public static MapReduceKeyFilter between(final int from, final int to,
				final boolean inclusive) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.between(json);
					node.add(from);
					node.add(to);
					node.add(inclusive);
				}
			};
		}

		public static MapReduceKeyFilter between(final long from, final long to) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.between(json);
					node.add(from);
					node.add(to);
				}
			};
		}

		public static MapReduceKeyFilter between(final long from,
				final long to, final boolean inclusive) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.between(json);
					node.add(from);
					node.add(to);
					node.add(inclusive);
				}
			};
		}

		public static MapReduceKeyFilter between(final double from,
				final double to) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.between(json);
					node.add(from);
					node.add(to);
				}
			};
		}

		public static MapReduceKeyFilter between(final double from,
				final double to, final boolean inclusive) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.between(json);
					node.add(from);
					node.add(to);
					node.add(inclusive);
				}
			};
		}

		public static MapReduceKeyFilter between(final float from,
				final float to) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.between(json);
					node.add(from);
					node.add(to);
				}
			};
		}

		public static MapReduceKeyFilter between(final float from,
				final float to, final boolean inclusive) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.between(json);
					node.add(from);
					node.add(to);
					node.add(inclusive);
				}
			};
		}
	}

	public static MapReduceKeyFilter and(MapReduceKeyFilter left,
			MapReduceKeyFilter right) {
		return null;
	}

}

class InternalFunctions {

	static ArrayNode create(String name, ArrayNode container) {
		ArrayNode node = container.addArray();
		node.add(name);
		return node;
	}

	static final MapReduceKeyFilter intToString = new MapReduceKeyFilter() {
		@Override
		public void appendTo(ArrayNode json) {
			create("int_to_string", json);
		}
	};

	static final MapReduceKeyFilter stringToInt = new MapReduceKeyFilter() {
		@Override
		public void appendTo(ArrayNode json) {
			create("string_to_int", json);
		}
	};

	static final MapReduceKeyFilter floatToString = new MapReduceKeyFilter() {
		@Override
		public void appendTo(ArrayNode json) {
			create("float_to_string", json);
		}
	};

	static final MapReduceKeyFilter stringToFloat = new MapReduceKeyFilter() {
		@Override
		public void appendTo(ArrayNode json) {
			create("string_to_float", json);
		}
	};
	static final MapReduceKeyFilter toUpper = new MapReduceKeyFilter() {
		@Override
		public void appendTo(ArrayNode json) {
			create("to_upper", json);
		}
	};

	static final MapReduceKeyFilter toLower = new MapReduceKeyFilter() {
		@Override
		public void appendTo(ArrayNode json) {
			create("to_lower", json);
		}
	};

	static final MapReduceKeyFilter urldecode = new MapReduceKeyFilter() {
		@Override
		public void appendTo(ArrayNode json) {
			create("urldecode", json);
		}
	};

	public static ArrayNode between(ArrayNode container) {
		return create("between", container);
	}

}