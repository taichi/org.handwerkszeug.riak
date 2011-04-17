package org.handwerkszeug.riak.model;

import org.codehaus.jackson.node.ObjectNode;

public interface Function {

	void appendTo(ObjectNode json);
}
