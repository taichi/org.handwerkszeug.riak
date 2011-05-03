package org.handwerkszeug.riak.http.rest;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.mapreduce.MapReduceResponse;

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
	public JsonNode getResponse() {
		if (this.node != null) {
			return this.node.get("data");
		}
		return null;
	}

	@Override
	public boolean getDone() {
		return this.done;
	}

}
