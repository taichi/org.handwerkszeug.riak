package org.handwerkszeug.riak.transport.protobuf.internal;

import java.io.IOException;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak.mapreduce.MapReduceResponse;
import org.handwerkszeug.riak.nls.Messages;
import org.handwerkszeug.riak.transport.protobuf.internal.RawProtoBufRiakclient.RpbMapRedResp;

import com.google.protobuf.ByteString;

/**
 * @author taichi
 */
public class ProtoBufMapReduceResponse implements MapReduceResponse {

	final RpbMapRedResp resp;

	public ProtoBufMapReduceResponse(RpbMapRedResp resp) {
		this.resp = resp;
	}

	@Override
	public Integer getPhase() {
		if (this.resp.hasPhase()) {
			return this.resp.getPhase();
		}
		return null;
	}

	@Override
	public ArrayNode getResponse() {
		if (this.resp.hasResponse()) {
			ByteString bs = this.resp.getResponse();
			JsonFactory factory = new JsonFactory(new ObjectMapper());
			try {
				JsonParser parser = factory.createJsonParser(bs.newInput());
				JsonNode jn = parser.readValueAsTree();
				if (jn instanceof ArrayNode) {
					ArrayNode an = (ArrayNode) jn;
					return an;
				} else {
					throw new IllegalStateException(String.format(
							Messages.MapReduceResponseMustBeArray, jn));
				}
			} catch (IOException e) {
				throw new RiakException(e);
			}
		}
		return null;
	}

	@Override
	public boolean getDone() {
		if (this.resp.hasDone()) {
			return this.resp.getDone();
		}
		return false;
	}

}
