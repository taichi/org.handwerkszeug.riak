package org.handwerkszeug.riak.op.mapreduce;

import org.codehaus.jackson.node.ArrayNode;
import org.handwerkszeug.riak.model.Location;

public class MapReduceLocationInput implements MapReduceInput {

	final Location location;

	public MapReduceLocationInput(Location location) {
		this.location = location;
	}

	@Override
	public void appendTo(ArrayNode json) {
		ArrayNode node = json.addArray();
		node.add(this.location.getBucket());
		node.add(this.location.getKey());
	}

}
