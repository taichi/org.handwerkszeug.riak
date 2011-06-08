package org.handwerkszeug.riak.transport.rest.internal;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;
import org.handwerkszeug.riak.model.Quorum;

/**
 * @author taichi
 */
public class QuorumJsonSerializer extends JsonSerializer<Quorum> {

	@Override
	public void serialize(Quorum value, JsonGenerator jgen,
			SerializerProvider provider) throws IOException,
			JsonProcessingException {
		if (Quorum.isNamed(value)) {
			jgen.writeString(value.getString());
		} else {
			jgen.writeNumber(value.getInteger());
		}
	}
}
