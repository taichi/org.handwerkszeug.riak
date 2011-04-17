package org.handwerkszeug.riak;

import org.codehaus.jackson.JsonNode;

/**
 * @author taichi
 */
public interface JsonAppender<T extends JsonNode> {

	void appendTo(T json);
}
