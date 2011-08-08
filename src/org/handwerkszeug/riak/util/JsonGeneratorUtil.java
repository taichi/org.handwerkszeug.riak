package org.handwerkszeug.riak.util;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.codehaus.jackson.JsonGenerator;

/**
 * @author taichi
 */
public class JsonGeneratorUtil {

	public static void writeArray(JsonGenerator generator, String name)
			throws IOException {
		generator.writeStartArray();
		generator.writeString(name);
		generator.writeEndArray();
	}

	public static void writeArray(JsonGenerator generator, String name,
			String primary, String... elements) throws IOException {
		generator.writeStartArray();
		generator.writeString(name);
		generator.writeString(primary);
		for (String s : elements) {
			generator.writeString(s);
		}
		generator.writeEndArray();
	}

	public static void writeArray(JsonGenerator generator, String name,
			int value) throws IOException {
		generator.writeStartArray();
		generator.writeString(name);
		generator.writeNumber(value);
		generator.writeEndArray();
	}

	public static void writeArray(JsonGenerator generator, String name,
			long value) throws IOException {
		generator.writeStartArray();
		generator.writeString(name);
		generator.writeNumber(value);
		generator.writeEndArray();
	}

	public static void writeArray(JsonGenerator generator, String name,
			double value) throws IOException {
		generator.writeStartArray();
		generator.writeString(name);
		generator.writeNumber(value);
		generator.writeEndArray();
	}

	public static void writeArray(JsonGenerator generator, String name,
			float value) throws IOException {
		generator.writeStartArray();
		generator.writeString(name);
		generator.writeNumber(value);
		generator.writeEndArray();
	}

	public static void writeArray(JsonGenerator generator, String name,
			BigInteger value) throws IOException {
		generator.writeStartArray();
		generator.writeString(name);
		generator.writeNumber(value);
		generator.writeEndArray();
	}

	public static void writeArray(JsonGenerator generator, String name,
			BigDecimal value) throws IOException {
		generator.writeStartArray();
		generator.writeString(name);
		generator.writeNumber(value);
		generator.writeEndArray();
	}

	public static void writeArray(JsonGenerator generator, String name,
			int primary, int... values) throws IOException {
		generator.writeStartArray();
		generator.writeString(name);
		generator.writeNumber(primary);
		for (int v : values) {
			generator.writeNumber(v);
		}
		generator.writeEndArray();
	}

	public static void writeArray(JsonGenerator generator, String name,
			Long primary, Long... values) throws IOException {
		generator.writeStartArray();
		generator.writeString(name);
		generator.writeNumber(primary);
		for (long v : values) {
			generator.writeNumber(v);
		}
		generator.writeEndArray();
	}

	public static void writeArray(JsonGenerator generator, String name,
			Double primary, Double... values) throws IOException {
		generator.writeStartArray();
		generator.writeString(name);
		generator.writeNumber(primary);
		for (double v : values) {
			generator.writeNumber(v);
		}
		generator.writeEndArray();
	}

	public static void writeArray(JsonGenerator generator, String name,
			Float primary, Float... values) throws IOException {
		generator.writeStartArray();
		generator.writeString(name);
		generator.writeNumber(primary);
		for (float v : values) {
			generator.writeNumber(v);
		}
		generator.writeEndArray();
	}

	public static void writeArray(JsonGenerator generator, String name,
			BigInteger primary, BigInteger... values) throws IOException {
		generator.writeStartArray();
		generator.writeString(name);
		generator.writeNumber(primary);
		for (BigInteger v : values) {
			generator.writeNumber(v);
		}
		generator.writeEndArray();
	}

	public static void writeArray(JsonGenerator generator, String name,
			BigDecimal primary, BigDecimal... values) throws IOException {
		generator.writeStartArray();
		generator.writeString(name);
		generator.writeNumber(primary);
		for (BigDecimal v : values) {
			generator.writeNumber(v);
		}
		generator.writeEndArray();
	}
}
