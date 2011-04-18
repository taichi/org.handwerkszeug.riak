package org.handwerkszeug.riak.model;

import org.codehaus.jackson.node.ObjectNode;

/**
 * @author taichi
 */
public class JavaScript implements Function {

	public static final String LANG = "javascript";

	final String name;

	public JavaScript(String name) {
		this.name = name;
	}

	@Override
	public String getLanguage() {
		return LANG;
	}

	@Override
	public void appendTo(ObjectNode json) {
		json.put("name", this.name);
	}

	// TODO list built-in functions
}
