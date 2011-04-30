package org.handwerkszeug.riak.util;

import org.codehaus.jackson.JsonNode;

/**
 * @author taichi
 */
public interface JsonAppender<T extends JsonNode> {

	void appendTo(T json);
}
