package org.handwerkszeug.riak.pbc;

import java.io.IOException;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak.mapreduce.MapReduceResponse;
import org.handwerkszeug.riak.pbc.Riakclient.RpbMapRedResp;

import com.google.protobuf.ByteString;

/**
 * @author taichi
 */
public class PbcMapReduceResponse implements MapReduceResponse {

	final RpbMapRedResp resp;

	public PbcMapReduceResponse(RpbMapRedResp resp) {
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
	public JsonNode getResponse() {
		if (resp.hasResponse()) {
			ByteString bs = resp.getResponse();
			JsonFactory factory = new JsonFactory(new ObjectMapper());
			try {
				JsonParser parser = factory.createJsonParser(bs.newInput());
				return parser.readValueAsTree();
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
