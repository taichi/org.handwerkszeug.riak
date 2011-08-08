package org.handwerkszeug.riak.transport.rest.internal;

import java.io.IOException;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.handwerkszeug.riak.model.Erlang;
import org.handwerkszeug.riak.model.Function;
import org.handwerkszeug.riak.model.JavaScript;
import org.handwerkszeug.riak.util.StringUtil;

/**
 * @author taichi
 */
public class FunctionJsonDeserializer extends JsonDeserializer<Function> {

	@Override
	public Function deserialize(JsonParser jp, DeserializationContext ctxt)
			throws IOException, JsonProcessingException {
		JsonToken jt = jp.getCurrentToken();
		if (JsonToken.START_OBJECT == jt) {
			Function result = null;
			String mod = null;
			String fun = null;
			jp.nextToken();
			for (; jp.getCurrentToken() != JsonToken.END_OBJECT; jp.nextToken()) {
				String name = jp.getCurrentName();
				jp.nextToken();
				String value = jp.getText();
				if ("name".equals(name)) {
					result = JavaScript.newFunction(value);
				}
				if ("mod".equals(name)) {
					mod = value;
				}
				if ("fun".equals(name)) {
					fun = value;
				}
				if (StringUtil.isEmpty(mod) == false
						&& StringUtil.isEmpty(fun) == false) {
					result = Erlang.newFunction(mod, fun);
				}
			}
			if (result != null) {
				return result;
			}
		}
		throw ctxt.mappingException(Function.class);
	}
}
