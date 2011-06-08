package org.handwerkszeug.riak.transport.rest.internal;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;
import org.handwerkszeug.riak.model.Erlang;
import org.handwerkszeug.riak.model.Function;
import org.handwerkszeug.riak.model.JavaScript;

/**
 * @author taichi
 */
public class FunctionJsonSerializer extends JsonSerializer<Function> {

	@Override
	public void serialize(Function value, JsonGenerator jgen,
			SerializerProvider provider) throws IOException,
			JsonProcessingException {
		jgen.writeStartObject();
		if (value instanceof JavaScript) {
			JavaScript js = (JavaScript) value;
			serialize(js, jgen, provider);
		}
		if (value instanceof Erlang) {
			Erlang e = (Erlang) value;
			serialize(e, jgen, provider);
		}
		jgen.writeEndObject();
	}

	protected void serialize(JavaScript function, JsonGenerator jgen,
			SerializerProvider provider) throws IOException,
			JsonProcessingException {
		jgen.writeStringField("name", function.getName());
	}

	protected void serialize(Erlang function, JsonGenerator jgen,
			SerializerProvider provider) throws IOException,
			JsonProcessingException {
		jgen.writeStringField("mod", function.getModule());
		jgen.writeStringField("fun", function.getFunction());
	}
}
