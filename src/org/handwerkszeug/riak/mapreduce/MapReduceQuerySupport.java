package org.handwerkszeug.riak.mapreduce;

import static org.handwerkszeug.riak.util.JsonGeneratorUtil.writeArray;
import static org.handwerkszeug.riak.util.Validation.notNull;
import static org.handwerkszeug.riak.util.Validation.positiveNumber;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

import org.codehaus.jackson.JsonGenerator;
import org.handwerkszeug.riak.mapreduce.internal.LocationInput;
import org.handwerkszeug.riak.model.Function;
import org.handwerkszeug.riak.model.Language;

/**
 * ease support functions. use static import.
 * 
 * @author taichi
 */
public class MapReduceQuerySupport {

	/*
	 * Inputs functions
	 */
	public static MapReduceInput location(String bucket, String key) {
		notNull(bucket, "bucket");
		notNull(key, "key");
		return new LocationInput(bucket, key);
	}

	public static MapReduceInput location(String bucket, String key,
			final Object keyData) {
		notNull(bucket, "bucket");
		notNull(key, "key");
		notNull(keyData, "keyData");
		return new LocationInput(bucket, key) {
			@Override
			protected void appendKeyData(JsonGenerator generator)
					throws IOException {
				generator.writeObject(keyData);
			}
		};
	}

	public static MapReduceInput location(String bucket, String key,
			final String keyData) {
		notNull(bucket, "bucket");
		notNull(key, "key");
		notNull(keyData, "keyData");
		return new LocationInput(bucket, key) {
			@Override
			protected void appendKeyData(JsonGenerator generator)
					throws IOException {
				generator.writeString(keyData);
			}
		};
	}

	public static MapReduceInput location(String bucket, String key,
			final int keyData) {
		notNull(bucket, "bucket");
		notNull(key, "key");
		return new LocationInput(bucket, key) {
			@Override
			protected void appendKeyData(JsonGenerator generator)
					throws IOException {
				generator.writeNumber(keyData);
			}
		};
	}

	public static MapReduceInput location(String bucket, String key,
			final long keyData) {
		notNull(bucket, "bucket");
		notNull(key, "key");
		return new LocationInput(bucket, key) {
			@Override
			protected void appendKeyData(JsonGenerator generator)
					throws IOException {
				generator.writeNumber(keyData);
			}
		};
	}

	public static MapReduceInput location(String bucket, String key,
			final double keyData) {
		notNull(bucket, "bucket");
		notNull(key, "key");
		return new LocationInput(bucket, key) {
			@Override
			protected void appendKeyData(JsonGenerator generator)
					throws IOException {
				generator.writeNumber(keyData);
			}
		};
	}

	public static MapReduceInput location(String bucket, String key,
			final float keyData) {
		notNull(bucket, "bucket");
		notNull(key, "key");
		return new LocationInput(bucket, key) {
			@Override
			protected void appendKeyData(JsonGenerator generator)
					throws IOException {
				generator.writeNumber(keyData);
			}
		};
	}

	public static MapReduceInput location(String bucket, String key,
			final BigInteger keyData) {
		notNull(bucket, "bucket");
		notNull(key, "key");
		notNull(keyData, "keyData");
		return new LocationInput(bucket, key) {
			@Override
			protected void appendKeyData(JsonGenerator generator)
					throws IOException {
				generator.writeNumber(keyData);
			}
		};
	}

	public static MapReduceInput location(String bucket, String key,
			final BigDecimal keyData) {
		notNull(bucket, "bucket");
		notNull(key, "key");
		notNull(keyData, "keyData");
		return new LocationInput(bucket, key) {
			@Override
			protected void appendKeyData(JsonGenerator generator)
					throws IOException {
				generator.writeNumber(keyData);
			}
		};
	}

	/*
	 * KeyFilter functions.
	 */

	public static Iterable<MapReduceKeyFilter> filters(
			MapReduceKeyFilter... filters) {
		return Arrays.asList(filters);
	}

	/**
	 * <b>Transform function</b>
	 * <p>
	 * Turns an integer (previously extracted with string_to_int), into a
	 * string.
	 * </p>
	 */
	public static final MapReduceKeyFilter intToString = new MapReduceKeyFilter() {
		@Override
		public void appendTo(JsonGenerator generator) throws IOException {
			writeArray(generator, "int_to_string");
		}
	};

	/**
	 * <b>Transform function</b>
	 * <p>
	 * Turns a string into an integer.
	 * </p>
	 */
	public static final MapReduceKeyFilter stringToInt = new MapReduceKeyFilter() {
		@Override
		public void appendTo(JsonGenerator generator) throws IOException {
			writeArray(generator, "string_to_int");
		}
	};

	/**
	 * <b>Transform function</b>
	 * <p>
	 * Turns a floating point number (previously extracted with
	 * string_to_float), into a string.
	 * </p>
	 */
	public static final MapReduceKeyFilter floatToString = new MapReduceKeyFilter() {
		@Override
		public void appendTo(JsonGenerator generator) throws IOException {
			writeArray(generator, "float_to_string");
		}
	};

	/**
	 * <b>Transform function</b>
	 * <p>
	 * Turns a string into a floating point number.
	 * </p>
	 */
	public static final MapReduceKeyFilter stringToFloat = new MapReduceKeyFilter() {
		@Override
		public void appendTo(JsonGenerator generator) throws IOException {
			writeArray(generator, "string_to_float");
		}
	};

	/**
	 * <b>Transform function</b>
	 * <p>
	 * Changes all letters to uppercase.
	 * </p>
	 */
	public static final MapReduceKeyFilter toUpper = new MapReduceKeyFilter() {
		@Override
		public void appendTo(JsonGenerator generator) throws IOException {
			writeArray(generator, "to_upper");
		}
	};

	/**
	 * <b>Transform function</b>
	 * <p>
	 * Changes all letters to lowercase.
	 * </p>
	 */
	public static final MapReduceKeyFilter toLower = new MapReduceKeyFilter() {
		@Override
		public void appendTo(JsonGenerator generator) throws IOException {
			writeArray(generator, "to_lower");
		}
	};

	/**
	 * <b>Transform function</b>
	 * <p>
	 * Splits the input on the string given as the first argument and returns
	 * the nth token specified by the second argument.
	 * </p>
	 * 
	 * @param delimiter
	 * @param nth
	 */
	public static MapReduceKeyFilter tokenize(final String delimiter,
			final int nth) {
		notNull(delimiter, "delimiter");
		positiveNumber(nth, "nth");
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				generator.writeStartArray();
				generator.writeString("tokenize");
				generator.writeString(delimiter);
				generator.writeNumber(nth);
				generator.writeEndArray();
			}
		};
	}

	/**
	 * <b>Transform function</b>
	 * <p>
	 * URL-decodes the string.
	 * </p>
	 */
	public static final MapReduceKeyFilter urldecode = new MapReduceKeyFilter() {
		@Override
		public void appendTo(JsonGenerator generator) throws IOException {
			writeArray(generator, "urldecode");
		}
	};

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is greater than the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter greaterThan(final String value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "greater_than", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is greater than the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter greaterThan(final int value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "greater_than", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is greater than the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter greaterThan(final long value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "greater_than", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is greater than the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter greaterThan(final double value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "greater_than", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is greater than the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter greaterThan(final float value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "greater_than", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is greater than the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter greaterThan(final BigInteger value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "greater_than", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is greater than the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter greaterThan(final BigDecimal value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "greater_than", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is less than the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter lessThan(final String value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "less_than", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is less than the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter lessThan(final int value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "less_than", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is less than the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter lessThan(final long value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "less_than", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is less than the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter lessThan(final float value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "less_than", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is less than the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter lessThan(final double value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "less_than", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is less than the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter lessThan(final BigInteger value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "less_than", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is less than the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter lessThan(final BigDecimal value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "less_than", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is greater than or equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter greaterThanEq(final String value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "greater_than_eq", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is greater than or equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter greaterThanEq(final int value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "greater_than_eq", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is greater than or equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter greaterThanEq(final long value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "greater_than_eq", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is greater than or equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter greaterThanEq(final double value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "greater_than_eq", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is greater than or equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter greaterThanEq(final float value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "greater_than_eq", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is greater than or equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter greaterThanEq(final BigInteger value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "greater_than_eq", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is greater than or equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter greaterThanEq(final BigDecimal value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "greater_than_eq", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is less than or equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter lessThanEq(final String value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "less_than_eq", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is less than or equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter lessThanEq(final int value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "less_than_eq", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is less than or equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter lessThanEq(final long value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "less_than_eq", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is less than or equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter lessThanEq(final float value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "less_than_eq", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is less than or equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter lessThanEq(final double value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "less_than_eq", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is less than or equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter lessThanEq(final BigInteger value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "less_than_eq", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is less than or equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter lessThanEq(final BigDecimal value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "less_than_eq", value);
			}
		};
	}

	private static void between(JsonGenerator generator) throws IOException {
		generator.writeStartArray();
		generator.writeString("between");
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is between the first two arguments. If the third
	 * argument is given, it is whether to treat the range as inclusive. If the
	 * third argument is omitted, the range is treated as inclusive.
	 * </p>
	 */
	public static MapReduceKeyFilter between(final String from, final String to) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				between(generator);
				generator.writeString(from);
				generator.writeString(to);
				generator.writeEndArray();
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is between the first two arguments. If the third
	 * argument is given, it is whether to treat the range as inclusive. If the
	 * third argument is omitted, the range is treated as inclusive.
	 * </p>
	 */
	public static MapReduceKeyFilter between(final String from,
			final String to, final boolean inclusive) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				between(generator);
				generator.writeString(from);
				generator.writeString(to);
				generator.writeBoolean(inclusive);
				generator.writeEndArray();
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is between the first two arguments. If the third
	 * argument is given, it is whether to treat the range as inclusive. If the
	 * third argument is omitted, the range is treated as inclusive.
	 * </p>
	 */
	public static MapReduceKeyFilter between(final int from, final int to) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				between(generator);
				generator.writeNumber(from);
				generator.writeNumber(to);
				generator.writeEndArray();
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is between the first two arguments. If the third
	 * argument is given, it is whether to treat the range as inclusive. If the
	 * third argument is omitted, the range is treated as inclusive.
	 * </p>
	 */
	public static MapReduceKeyFilter between(final int from, final int to,
			final boolean inclusive) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				between(generator);
				generator.writeNumber(from);
				generator.writeNumber(to);
				generator.writeBoolean(inclusive);
				generator.writeEndArray();
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is between the first two arguments. If the third
	 * argument is given, it is whether to treat the range as inclusive. If the
	 * third argument is omitted, the range is treated as inclusive.
	 * </p>
	 */
	public static MapReduceKeyFilter between(final long from, final long to) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				between(generator);
				generator.writeNumber(from);
				generator.writeNumber(to);
				generator.writeEndArray();
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is between the first two arguments. If the third
	 * argument is given, it is whether to treat the range as inclusive. If the
	 * third argument is omitted, the range is treated as inclusive.
	 * </p>
	 */
	public static MapReduceKeyFilter between(final long from, final long to,
			final boolean inclusive) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				between(generator);
				generator.writeNumber(from);
				generator.writeNumber(to);
				generator.writeBoolean(inclusive);
				generator.writeEndArray();
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is between the first two arguments. If the third
	 * argument is given, it is whether to treat the range as inclusive. If the
	 * third argument is omitted, the range is treated as inclusive.
	 * </p>
	 */
	public static MapReduceKeyFilter between(final double from, final double to) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				between(generator);
				generator.writeNumber(from);
				generator.writeNumber(to);
				generator.writeEndArray();
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is between the first two arguments. If the third
	 * argument is given, it is whether to treat the range as inclusive. If the
	 * third argument is omitted, the range is treated as inclusive.
	 * </p>
	 */
	public static MapReduceKeyFilter between(final double from,
			final double to, final boolean inclusive) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				between(generator);
				generator.writeNumber(from);
				generator.writeNumber(to);
				generator.writeBoolean(inclusive);
				generator.writeEndArray();
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is between the first two arguments. If the third
	 * argument is given, it is whether to treat the range as inclusive. If the
	 * third argument is omitted, the range is treated as inclusive.
	 * </p>
	 */
	public static MapReduceKeyFilter between(final float from, final float to) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				between(generator);
				generator.writeNumber(from);
				generator.writeNumber(to);
				generator.writeEndArray();
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is between the first two arguments. If the third
	 * argument is given, it is whether to treat the range as inclusive. If the
	 * third argument is omitted, the range is treated as inclusive.
	 * </p>
	 */
	public static MapReduceKeyFilter between(final float from, final float to,
			final boolean inclusive) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				between(generator);
				generator.writeNumber(from);
				generator.writeNumber(to);
				generator.writeBoolean(inclusive);
				generator.writeEndArray();
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is between the first two arguments. If the third
	 * argument is given, it is whether to treat the range as inclusive. If the
	 * third argument is omitted, the range is treated as inclusive.
	 * </p>
	 */
	public static MapReduceKeyFilter between(final BigInteger from,
			final BigInteger to) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				between(generator);
				generator.writeNumber(from);
				generator.writeNumber(to);
				generator.writeEndArray();
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is between the first two arguments. If the third
	 * argument is given, it is whether to treat the range as inclusive. If the
	 * third argument is omitted, the range is treated as inclusive.
	 * </p>
	 */
	public static MapReduceKeyFilter between(final BigInteger from,
			final BigInteger to, final boolean inclusive) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				between(generator);
				generator.writeNumber(from);
				generator.writeNumber(to);
				generator.writeBoolean(inclusive);
				generator.writeEndArray();
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is between the first two arguments. If the third
	 * argument is given, it is whether to treat the range as inclusive. If the
	 * third argument is omitted, the range is treated as inclusive.
	 * </p>
	 */
	public static MapReduceKeyFilter between(final BigDecimal from,
			final BigDecimal to) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				between(generator);
				generator.writeNumber(from);
				generator.writeNumber(to);
				generator.writeEndArray();
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is between the first two arguments. If the third
	 * argument is given, it is whether to treat the range as inclusive. If the
	 * third argument is omitted, the range is treated as inclusive.
	 * </p>
	 */
	public static MapReduceKeyFilter between(final BigDecimal from,
			final BigDecimal to, final boolean inclusive) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				between(generator);
				generator.writeNumber(from);
				generator.writeNumber(to);
				generator.writeBoolean(inclusive);
				generator.writeEndArray();
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input matches the regular expression given in the
	 * argument.
	 * </p>
	 */
	public static MapReduceKeyFilter matches(final String regex) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "matches", regex);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is not equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter notEqual(final String value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "neq", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is not equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter notEqual(final int value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "neq", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is not equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter notEqual(final long value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "neq", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is not equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter notEqual(final double value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "neq", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is not equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter notEqual(final float value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "neq", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is not equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter notEqual(final BigInteger value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "neq", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is not equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter notEqual(final BigDecimal value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "neq", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter equal(final String value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "eq", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter equal(final int value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "eq", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter equal(final long value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "eq", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter equal(final double value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "eq", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter equal(final float value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "eq", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter equal(final BigInteger value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "eq", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is equal to the argument.
	 * </p>
	 */
	public static MapReduceKeyFilter equal(final BigDecimal value) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "eq", value);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is contained in the set given as the arguments.
	 * </p>
	 */
	public static MapReduceKeyFilter setMember(final String primary,
			final String... values) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "set_member", primary, values);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is contained in the set given as the arguments.
	 * </p>
	 */
	public static MapReduceKeyFilter setMember(final int primary,
			final int... values) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "set_member", primary, values);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is contained in the set given as the arguments.
	 * </p>
	 */
	public static MapReduceKeyFilter setMember(final Long primary,
			final Long... values) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "set_member", primary, values);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is contained in the set given as the arguments.
	 * </p>
	 */
	public static MapReduceKeyFilter setMember(final Double primary,
			final Double... values) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "set_member", primary, values);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is contained in the set given as the arguments.
	 * </p>
	 */
	public static MapReduceKeyFilter setMember(final Float primary,
			final Float... values) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "set_member", primary, values);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is contained in the set given as the arguments.
	 * </p>
	 */
	public static MapReduceKeyFilter setMember(final BigInteger primary,
			final BigInteger... values) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "set_member", primary, values);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input is contained in the set given as the arguments.
	 * </p>
	 */
	public static MapReduceKeyFilter setMember(final BigDecimal primary,
			final BigDecimal... values) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "set_member", primary, values);
			}
		};
	}

	/**
	 * <b>Predicate function</b> Tests that input is within the <a
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
			public void appendTo(JsonGenerator generator) throws IOException {
				generator.writeStartArray();
				generator.writeString("similar_to");
				generator.writeString(similar);
				generator.writeNumber(distance);
				generator.writeEndArray();
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input begins with the argument (a string).
	 * </p>
	 */
	public static MapReduceKeyFilter startsWith(final String startsWith) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "starts_with", startsWith);
			}
		};
	}

	/**
	 * <b>Predicate function</b>
	 * <p>
	 * Tests that the input ends with the argument (a string).
	 * </p>
	 */
	public static MapReduceKeyFilter endsWith(final String endsWith) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeArray(generator, "ends_with", endsWith);
			}
		};
	}

	/**
	 * <b>Logical operation</b>
	 * <p>
	 * Joins two or more key-filter operations with a logical AND operation.
	 * </p>
	 * 
	 * @see #filters(MapReduceKeyFilter...)
	 */
	public static MapReduceKeyFilter and(
			final Iterable<MapReduceKeyFilter> lefts,
			final Iterable<MapReduceKeyFilter> rights) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeLogical(generator, "and", rights, lefts);
			}
		};
	}

	private static void writeLogical(JsonGenerator generator, String name,
			Iterable<MapReduceKeyFilter> rights,
			Iterable<MapReduceKeyFilter> lefts) throws IOException {
		generator.writeStartArray();
		generator.writeString(name);
		writeKeyFilters(generator, lefts);
		writeKeyFilters(generator, rights);
		generator.writeEndArray();
	}

	private static void writeKeyFilters(JsonGenerator generator,
			Iterable<MapReduceKeyFilter> filters) throws IOException {
		generator.writeStartArray();
		for (MapReduceKeyFilter f : filters) {
			f.appendTo(generator);
		}
		generator.writeEndArray();
	}

	/**
	 * <b>Logical operation</b>
	 * <p>
	 * Joins two or more key-filter operations with a logical OR operation.
	 * </p>
	 * 
	 * @see #filters(MapReduceKeyFilter...)
	 */
	public static MapReduceKeyFilter or(
			final Iterable<MapReduceKeyFilter> lefts,
			final Iterable<MapReduceKeyFilter> rights) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				writeLogical(generator, "or", rights, lefts);
			}
		};
	}

	/**
	 * <b>Logical operation</b>
	 * <p>
	 * Negates the result of key-filter operations.
	 * </p>
	 * 
	 * @see #filters(MapReduceKeyFilter...)
	 */
	public static MapReduceKeyFilter not(
			final Iterable<MapReduceKeyFilter> filters) {
		return new MapReduceKeyFilter() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				generator.writeStartArray();
				generator.writeString("not");
				generator.writeStartArray();
				for (MapReduceKeyFilter f : filters) {
					f.appendTo(generator);
				}
				generator.writeEndArray();
				generator.writeEndArray();
			}
		};
	}

	/*
	 * <b>Phase function</b>
	 */
	static Function adHoc(Language lang, final String source) {
		return new Function(lang) {
			@Override
			protected void appendBody(JsonGenerator generator)
					throws IOException {
				generator.writeStringField("source", source);
			}
		};
	}

	/**
	 * <b>Phase function</b>
	 * <p>
	 * adHoc javascript function<br/>
	 * use only development.
	 * </p>
	 * 
	 * @see #stored(Language, String, String)
	 */
	public static Function js(String source) {
		return adHoc(Language.javascript, source);
	}

	/**
	 * <b>Phase function</b>
	 * <p>
	 * adHoc erlang function<br/>
	 * use only development.
	 * </p>
	 * 
	 * @see #stored(Language, String, String)
	 */
	public static Function erl(String source) {
		return adHoc(Language.erlang, source);
	}

	/**
	 * <b>Phase function</b>
	 * <p>
	 * </p>
	 */
	public static Function stored(Language lang, final String bucket,
			final String key) {
		return new Function(lang) {
			@Override
			protected void appendBody(JsonGenerator generator)
					throws IOException {
				generator.writeStringField("bucket", bucket);
				generator.writeStringField("key", key);
			}
		};
	}

}
