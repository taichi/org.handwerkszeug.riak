package org.handwerkszeug.riak.mapreduce;

import org.codehaus.jackson.node.ArrayNode;
import org.handwerkszeug.riak.model.Location;

/**
 * The list of input objects is given as a list of 2-element lists of the form
 * [Bucket,Key] or 3-element lists of the form [Bucket,Key,KeyData].
 * 
 * @author taichi
 */
public class MapReduceLocationInput implements MapReduceInput {

	final Location location;
	final Object keydata;

	public MapReduceLocationInput(Location location) {
		this(location, null);
	}

	public MapReduceLocationInput(Location location, Object keydata) {
		this.location = location;
		this.keydata = keydata;

	}

	@Override
	public void appendTo(ArrayNode json) {
		ArrayNode node = json.addArray();
		node.add(this.location.getBucket());
		node.add(this.location.getKey());
		if (keydata != null) {
			node.addPOJO(this.keydata);
		}
	}

}
