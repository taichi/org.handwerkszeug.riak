package org.handwerkszeug.riak.op.mapreduce;

import java.util.regex.Pattern;

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

		public static MapReduceKeyFilter greaterThan(final String value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.greaterThan(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter greaterThan(final int value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.greaterThan(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter greaterThan(final long value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.greaterThan(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter greaterThan(final double value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.greaterThan(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter greaterThan(final float value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.greaterThan(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter lessThan(final String value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.lessThan(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter lessThan(final int value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.lessThan(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter lessThan(final long value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.lessThan(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter lessThan(final double value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.lessThan(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter lessThan(final float value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.lessThan(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter greaterThanEq(final String value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.greaterThanEq(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter greaterThanEq(final int value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.greaterThanEq(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter greaterThanEq(final long value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.greaterThanEq(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter greaterThanEq(final double value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.greaterThanEq(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter greaterThanEq(final float value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.greaterThanEq(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter lessThanEq(final String value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.lessThanEq(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter lessThanEq(final int value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.lessThanEq(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter lessThanEq(final long value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.lessThanEq(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter lessThanEq(final double value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.lessThanEq(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter lessThanEq(final float value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.lessThanEq(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter notEqual(final String value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.notEqual(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter notEqual(final int value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.notEqual(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter notEqual(final long value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.notEqual(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter notEqual(final double value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.notEqual(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter notEqual(final float value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.notEqual(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter equal(final String value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.equal(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter equal(final int value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.equal(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter equal(final long value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.equal(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter equal(final double value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.equal(json);
					node.add(value);
				}
			};
		}

		public static MapReduceKeyFilter equal(final float value) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.equal(json);
					node.add(value);
				}
			};
		}

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

		public static MapReduceKeyFilter matches(final String regex) {
			// TODO validate expression.
			// does Riak support java compatible regular expressions ?
			Pattern.compile(regex);
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.matches(json);
					node.add(regex);
				}
			};
		}

		/**
		 * Tests that the input is contained in the set given as the arguments.
		 */
		public static MapReduceKeyFilter setMember(final String[] members) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.setMember(json);
					for (String member : members) {
						node.add(member);
					}
				}
			};
		}

		/**
		 * Tests that the input is contained in the set given as the arguments.
		 */
		public static MapReduceKeyFilter setMember(final int[] members) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.setMember(json);
					for (int member : members) {
						node.add(member);
					}
				}
			};
		}

		/**
		 * Tests that the input is contained in the set given as the arguments.
		 */
		public static MapReduceKeyFilter setMember(final long[] members) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.setMember(json);
					for (long member : members) {
						node.add(member);
					}
				}
			};
		}

		/**
		 * Tests that the input is contained in the set given as the arguments.
		 */
		public static MapReduceKeyFilter setMember(final double[] members) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.setMember(json);
					for (double member : members) {
						node.add(member);
					}
				}
			};
		}

		/**
		 * Tests that the input is contained in the set given as the arguments.
		 */
		public static MapReduceKeyFilter setMember(final float[] members) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.setMember(json);
					for (float member : members) {
						node.add(member);
					}
				}
			};
		}

		/**
		 * Tests that input is within the <a
		 * href="http://en.wikipedia.org/wiki/Levenshtein_distance">Levenshtein
		 * distance</a> of the first argument given by the second argument.
		 * 
		 * @param similar
		 * @param distance
		 */
		public static MapReduceKeyFilter similarTo(final String similar,
				final int distance) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.similarTo(json);
					node.add(similar);
					node.add(distance);
				}
			};
		}

		public static MapReduceKeyFilter startsWith(final String startsWith) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.startsWith(json);
					node.add(startsWith);
				}
			};
		}

		public static MapReduceKeyFilter endsWith(final String endsWith) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.endsWith(json);
					node.add(endsWith);
				}
			};
		}

		public static MapReduceKeyFilter and(final MapReduceKeyFilter left,
				final MapReduceKeyFilter right) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.and(json);
					left.appendTo(node.addArray());
					right.appendTo(node.addArray());
				}
			};
		}

		public static MapReduceKeyFilter or(final MapReduceKeyFilter left,
				final MapReduceKeyFilter right) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.or(json);
					left.appendTo(node.addArray());
					right.appendTo(node.addArray());
				}
			};
		}

		public static MapReduceKeyFilter not(final MapReduceKeyFilter left) {
			return new MapReduceKeyFilter() {
				@Override
				public void appendTo(ArrayNode json) {
					ArrayNode node = InternalFunctions.not(json);
					left.appendTo(node.addArray());
				}
			};
		}
	}
}

class InternalFunctions {

	static ArrayNode create(String name, ArrayNode container) {
		ArrayNode node = container.addArray();
		node.add(name);
		return node;
	}

	/*
	 * transform functions
	 */

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

	/*
	 * predicates
	 */

	public static ArrayNode greaterThan(ArrayNode container) {
		return create("greater_than", container);
	}

	public static ArrayNode lessThan(ArrayNode container) {
		return create("less_than", container);
	}

	public static ArrayNode greaterThanEq(ArrayNode container) {
		return create("greater_than_eq", container);
	}

	public static ArrayNode lessThanEq(ArrayNode container) {
		return create("less_than_eq", container);
	}

	public static ArrayNode notEqual(ArrayNode container) {
		return create("neq", container);
	}

	public static ArrayNode equal(ArrayNode container) {
		return create("eq", container);
	}

	public static ArrayNode between(ArrayNode container) {
		return create("between", container);
	}

	public static ArrayNode matches(ArrayNode container) {
		return create("matches", container);
	}

	public static ArrayNode setMember(ArrayNode container) {
		return create("set_member", container);
	}

	public static ArrayNode similarTo(ArrayNode container) {
		return create("similar_to", container);
	}

	public static ArrayNode startsWith(ArrayNode container) {
		return create("starts_with", container);
	}

	public static ArrayNode endsWith(ArrayNode container) {
		return create("ends_with", container);
	}

	public static ArrayNode and(ArrayNode container) {
		return create("and", container);
	}

	public static ArrayNode or(ArrayNode container) {
		return create("or", container);
	}

	public static ArrayNode not(ArrayNode container) {
		return create("not", container);
	}
}