package org.handwerkszeug.riak.util;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerator;

/**
 * @author taichi
 */
public interface JsonAppender {

	void appendTo(JsonGenerator generator) throws IOException;
}
