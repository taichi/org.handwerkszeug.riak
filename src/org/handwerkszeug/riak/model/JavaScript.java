package org.handwerkszeug.riak.model;

import org.codehaus.jackson.node.ObjectNode;

/**
 * @author taichi
 */
public class JavaScript implements Function {

	final String name;

	public JavaScript(String name) {
		this.name = name;
	}

	@Override
	public void appendTo(ObjectNode json) {
		json.put("name", this.name);
	}
}
