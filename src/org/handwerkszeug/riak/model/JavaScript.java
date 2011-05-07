package org.handwerkszeug.riak.model;

import static org.handwerkszeug.riak.util.Validation.notNull;

import org.codehaus.jackson.node.ObjectNode;

/**
 * @author taichi
 */
public class JavaScript implements Function {

	public static final String LANG = "javascript";

	final String name;

	public JavaScript(String name) {
		notNull(name, "name");
		this.name = name;
	}

	@Override
	public String getLanguage() {
		return LANG;
	}

	public String getName() {
		return this.name;
	}

	@Override
	public void appendTo(ObjectNode json) {
		json.put("name", this.name);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("JavaScript [name=");
		builder.append(name);
		builder.append("]");
		return builder.toString();
	}

	/*
	 * built-in functions
	 */

	public static final JavaScript mapValues = new JavaScript("Riak.mapValues");
	public static final JavaScript mapValuesJson = new JavaScript(
			"Riak.mapValuesJson");
	public static final JavaScript mapByFields = new JavaScript(
			"Riak.mapByFields");
	public static final JavaScript reduceSum = new JavaScript("Riak.reduceSum");
	public static final JavaScript reduceMin = new JavaScript("Riak.reduceMin");
	public static final JavaScript reduceMax = new JavaScript("Riak.reduceMax");
	public static final JavaScript reduceSort = new JavaScript(
			"Riak.reduceSort");
	public static final JavaScript reduceNumericSort = new JavaScript(
			"Riak.reduceNumericSort");
	public static final JavaScript reduceLimit = new JavaScript(
			"Riak.reduceLimit");
	public static final JavaScript reduceSlice = new JavaScript(
			"Riak.reduceSlice");
}
