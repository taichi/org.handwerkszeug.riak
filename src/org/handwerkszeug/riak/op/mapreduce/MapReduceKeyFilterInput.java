package org.handwerkszeug.riak.op.mapreduce;

import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

public class MapReduceKeyFilterInput implements MapReduceInput {

	String bucket;

	Iterable<MapReduceKeyFilter> keyFilters;

	public MapReduceKeyFilterInput(String bucket,
			Iterable<MapReduceKeyFilter> keyFilters) {
		this.bucket = bucket;
		this.keyFilters = keyFilters;
	}

	@Override
	public void appendTo(ArrayNode json) {
		ObjectNode node = json.addObject();
		node.put("bucket", this.bucket);
		ArrayNode array = node.putArray("key_filters");
		for (MapReduceKeyFilter kf : this.keyFilters) {
			kf.appendTo(array);
		}
	}
}
