package org.handwerkszeug.riak.op.mapreduce;

import org.codehaus.jackson.node.ArrayNode;

/**
 * @author taichi
 * @see <a
 *      href="https://github.com/basho/riak_kv/blob/master/src/riak_kv_mapred_filters.erl">riak_kv_mapred_filters.erl</a>
 */
public class MapReduceKeyFilters {

	/**
	 * Transform functions
	 */
	public static class Transform {
		static final MapReduceKeyFilter intToString = new MapReduceKeyFilter() {
			@Override
			public void appendTo(ArrayNode json) {
				create("int_to_string", json);
			}
		};

		/**
		 * Turns an integer (previously extracted with string_to_int), into a
		 * string.
		 */
		public static MapReduceKeyFilter intToString() {
			return intToString;
		}

		static final MapReduceKeyFilter stringToInt = new MapReduceKeyFilter() {
			@Override
			public void appendTo(ArrayNode json) {
				create("string_to_int", json);
			}
		};

		/**
		 * Turns a string into an integer.
		 */
		public static MapReduceKeyFilter stringToInt() {
			return stringToInt;
		}

		static final MapReduceKeyFilter floatToString = new MapReduceKeyFilter() {
			@Override
			public void appendTo(ArrayNode json) {
				create("float_to_string", json);
			}
		};

		/**
		 * Turns a floating point number (previously extracted with
		 * string_to_float), into a string.
		 */
		public static MapReduceKeyFilter floatToString() {
			return floatToString;
		}

		static final MapReduceKeyFilter stringToFloat = new MapReduceKeyFilter() {
			@Override
			public void appendTo(ArrayNode json) {
				create("string_to_float", json);
			}
		};

		/**
		 * Turns a string into a floating point number.
		 */
		public static MapReduceKeyFilter stringToFloat() {
			return stringToFloat;
		}

		static final MapReduceKeyFilter toUpper = new MapReduceKeyFilter() {
			@Override
			public void appendTo(ArrayNode json) {
				create("to_upper", json);
			}
		};

		/**
		 * Changes all letters to uppercase.
		 */
		public static MapReduceKeyFilter toUpper() {
			return toUpper;
		}

		static final MapReduceKeyFilter toLower = new MapReduceKeyFilter() {
			@Override
			public void appendTo(ArrayNode json) {
				create("to_lower", json);
			}
		};

		/**
		 * Changes all letters to lowercase.
		 */
		public static MapReduceKeyFilter toLower() {
			return toLower;
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
					ArrayNode node = create("tokenize", json);
					node.add(delimiter);
					node.add(nth);

				}
			};
		}

		static final MapReduceKeyFilter urldecode = new MapReduceKeyFilter() {
			@Override
			public void appendTo(ArrayNode json) {
				create("urldecode", json);
			}
		};

		/**
		 * URL-decodes the string.
		 */
		public static MapReduceKeyFilter urldecode() {
			return urldecode;
		}
	}

	/**
	 * Predicate functions
	 */
	public static class Predicates {

		static ArrayNode between(ArrayNode container) {
			return create("between", container);
		}

		public static MapReduceKeyFilter between(final String from,
				final String to) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = between(json);
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
					ArrayNode node = between(json);
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
					ArrayNode node = between(json);
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
					ArrayNode node = between(json);
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
					ArrayNode node = between(json);
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
					ArrayNode node = between(json);
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
					ArrayNode node = between(json);
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
					ArrayNode node = between(json);
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
					ArrayNode node = between(json);
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
					ArrayNode node = between(json);
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

	protected static ArrayNode create(String name, ArrayNode container) {
		ArrayNode node = container.addArray();
		node.add(name);
		return node;
	}
}
