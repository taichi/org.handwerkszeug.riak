package org.handwerkszeug.riak.model;

import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.JsonAppender;

public interface Function extends JsonAppender<ObjectNode> {

	String getLanguage();

}
