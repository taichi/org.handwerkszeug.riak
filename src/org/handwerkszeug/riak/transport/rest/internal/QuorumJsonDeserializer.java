package org.handwerkszeug.riak.transport.rest.internal;

import java.io.IOException;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.handwerkszeug.riak.model.Quorum;

/**
 * @author taichi
 */
public class QuorumJsonDeserializer extends JsonDeserializer<Quorum> {

	@Override
	public Quorum deserialize(JsonParser jp, DeserializationContext ctxt)
			throws IOException, JsonProcessingException {
		JsonToken jt = jp.getCurrentToken();
		switch (jt) {
		case VALUE_STRING: {
			return Quorum.of(jp.getText());
		}
		case VALUE_NUMBER_INT: {
			return Quorum.of(jp.getIntValue());
		}
		case VALUE_NULL: {
			return null;
		}
		default:
			break;
		}
		throw ctxt.mappingException(Quorum.class);
	}
}
