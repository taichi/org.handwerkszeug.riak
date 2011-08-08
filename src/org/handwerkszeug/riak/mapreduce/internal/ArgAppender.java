package org.handwerkszeug.riak.mapreduce.internal;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.codehaus.jackson.JsonGenerator;
import org.handwerkszeug.riak.model.Function;
import org.handwerkszeug.riak.util.JsonAppender;

/**
 * @author taichi
 */
public abstract class ArgAppender implements JsonAppender {

	static final String ARG = "arg";
	final Function function;

	protected ArgAppender(Function function) {
		this.function = function;
	}

	@Override
	public void appendTo(JsonGenerator generator) throws IOException {
		this.function.appendTo(generator);
		appendArg(generator);
	}

	protected abstract void appendArg(JsonGenerator generator)
			throws IOException;

	public static JsonAppender arg(Function function, final Object arg) {
		return new ArgAppender(function) {
			@Override
			public void appendArg(JsonGenerator generator) throws IOException {
				generator.writeObjectField(ARG, arg);
			}
		};
	}

	public static JsonAppender arg(Function function, final String arg) {
		return new ArgAppender(function) {
			@Override
			public void appendArg(JsonGenerator generator) throws IOException {
				generator.writeStringField(ARG, arg);
			}
		};
	}

	public static JsonAppender arg(Function function, final int arg) {
		return new ArgAppender(function) {
			@Override
			public void appendArg(JsonGenerator generator) throws IOException {
				generator.writeNumberField(ARG, arg);
			}
		};
	}

	public static JsonAppender arg(Function function, final long arg) {
		return new ArgAppender(function) {
			@Override
			public void appendArg(JsonGenerator generator) throws IOException {
				generator.writeNumberField(ARG, arg);
			}
		};
	}

	public static JsonAppender arg(Function function, final double arg) {
		return new ArgAppender(function) {
			@Override
			public void appendArg(JsonGenerator generator) throws IOException {
				generator.writeNumberField(ARG, arg);
			}
		};
	}

	public static JsonAppender arg(Function function, final float arg) {
		return new ArgAppender(function) {
			@Override
			public void appendArg(JsonGenerator generator) throws IOException {
				generator.writeNumberField(ARG, arg);
			}
		};
	}

	public static JsonAppender arg(Function function, final BigInteger arg) {
		return new ArgAppender(function) {
			@Override
			public void appendArg(JsonGenerator generator) throws IOException {
				generator.writeFieldName(ARG);
				generator.writeNumber(arg);
			}
		};
	}

	public static JsonAppender arg(Function function, final BigDecimal arg) {
		return new ArgAppender(function) {
			@Override
			public void appendArg(JsonGenerator generator) throws IOException {
				generator.writeFieldName(ARG);
				generator.writeNumber(arg);
			}
		};
	}

	public static JsonAppender arg(Function function, final String... args) {
		return new ArgAppender(function) {
			@Override
			public void appendArg(JsonGenerator generator) throws IOException {
				generator.writeArrayFieldStart(ARG);
				for (String s : args) {
					generator.writeString(s);
				}
				generator.writeEndArray();
			}
		};
	}
}