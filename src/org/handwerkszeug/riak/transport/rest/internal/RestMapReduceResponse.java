package org.handwerkszeug.riak.transport.rest.internal;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.mapreduce.MapReduceResponse;
import org.handwerkszeug.riak.nls.Messages;

/**
 * @author taichi
 */
public class RestMapReduceResponse implements MapReduceResponse {

	ObjectNode node;
	boolean done;

	public RestMapReduceResponse(ObjectNode node, boolean done) {
		this.node = node;
		this.done = done;
	}

	@Override
	public Integer getPhase() {
		if (this.node != null) {
			JsonNode jn = this.node.get("phase");
			if (jn.isInt()) {
				return ((IntNode) jn).getIntValue();
			}
		}
		return Integer.valueOf(-1);
	}

	@Override
	public ArrayNode getResponse() {
		if (this.node != null) {
			JsonNode jn = this.node.get("data");
			if (jn instanceof ArrayNode) {
				ArrayNode an = (ArrayNode) jn;
				return an;
			} else {
				throw new IllegalStateException(String.format(
						Messages.MapReduceResponseMustBeArray, jn));
			}
		}
		return null;
	}

	@Override
	public boolean getDone() {
		return this.done;
	}

}
