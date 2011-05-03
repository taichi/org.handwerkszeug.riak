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

	// TODO list built-in functions
}
