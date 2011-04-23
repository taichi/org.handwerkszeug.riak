package org.handwerkszeug.riak.mapreduce;

import java.util.Arrays;

import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.JsonAppender;
import org.handwerkszeug.riak.model.Location;

/**
 * @author taichi
 */
public abstract class MapReduceInput implements JsonAppender<ArrayNode> {

	public static MapReduceInput location(Location location) {
		return new LocationInput(location, null);
	}

	public static MapReduceInput location(Location location, Object keydata) {
		return new LocationInput(location, keydata);
	}

	public static MapReduceInput keyFilter(final String bucket,
			final MapReduceKeyFilter... keyFilters) {
		return keyFilter(bucket, Arrays.asList(keyFilters));
	}

	public static MapReduceInput keyFilter(final String bucket,
			final Iterable<MapReduceKeyFilter> keyFilters) {
		return new MapReduceInput() {
			@Override
			public void appendTo(ArrayNode json) {
				ObjectNode node = json.addObject();
				node.put("bucket", bucket);
				ArrayNode array = node.putArray("key_filters");
				for (MapReduceKeyFilter kf : keyFilters) {
					kf.appendTo(array);
				}
			}
		};
	}

	/**
	 * The list of input objects is given as a list of 2-element lists of the
	 * form [Bucket,Key] or 3-element lists of the form [Bucket,Key,KeyData].
	 */
	protected static class LocationInput extends MapReduceInput {

		final Location location;
		final Object keydata;

		public LocationInput(Location location, Object keydata) {
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
}
