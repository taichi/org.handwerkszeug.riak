package org.handwerkszeug.riak.model;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerator;

public abstract class JavaScript extends Function {

	protected String name;

	protected JavaScript(String name) {
		super(Language.javascript);
		this.name = name;
	}

	public String getName() {
		return this.name;
	}

	public static JavaScript newFunction(final String name) {
		return new JavaScript(name) {
			@Override
			protected void appendBody(JsonGenerator generator)
					throws IOException {
				generator.writeStringField("name", this.name);
			}
		};
	}

	/*
	 * BuiltIn Functions
	 */
	public static final JavaScript mapValues = newFunction("Riak.mapValues");
	public static final JavaScript mapValuesJson = newFunction("Riak.mapValuesJson");
	public static final JavaScript mapByFields = newFunction("Riak.mapByFields");
	public static final JavaScript reduceSum = newFunction("Riak.reduceSum");
	public static final JavaScript reduceMin = newFunction("Riak.reduceMin");
	public static final JavaScript reduceMax = newFunction("Riak.reduceMax");
	public static final JavaScript reduceSort = newFunction("Riak.reduceSort");
	public static final JavaScript reduceNumericSort = newFunction("Riak.reduceNumericSort");
	public static final JavaScript reduceLimit = newFunction("Riak.reduceLimit");
	public static final JavaScript reduceSlice = newFunction("Riak.reduceSlice");

}
