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
		return new LocationInput(location);
	}

	public static MapReduceInput location(Location location,
			final Object keydata) {
		return new LocationInput(location) {
			@Override
			protected void appendKeyData(ArrayNode node) {
				node.addPOJO(keydata);
			}
		};
	}

	public static MapReduceInput location(Location location,
			final String keydata) {
		return new LocationInput(location) {
			@Override
			protected void appendKeyData(ArrayNode node) {
				node.add(keydata);
			}
		};
	}

	public static MapReduceInput location(Location location, final int keydata) {
		return new LocationInput(location) {
			@Override
			protected void appendKeyData(ArrayNode node) {
				node.add(keydata);
			}
		};
	}

	public static MapReduceInput location(Location location, final long keydata) {
		return new LocationInput(location) {
			@Override
			protected void appendKeyData(ArrayNode node) {
				node.add(keydata);
			}
		};
	}

	public static MapReduceInput location(Location location,
			final double keydata) {
		return new LocationInput(location) {
			@Override
			protected void appendKeyData(ArrayNode node) {
				node.add(keydata);
			}
		};
	}

	public static MapReduceInput location(Location location, final float keydata) {
		return new LocationInput(location) {
			@Override
			protected void appendKeyData(ArrayNode node) {
				node.add(keydata);
			}
		};
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

		public LocationInput(Location location) {
			this.location = location;
		}

		@Override
		public void appendTo(ArrayNode json) {
			ArrayNode node = json.addArray();
			node.add(this.location.getBucket());
			node.add(this.location.getKey());
			appendKeyData(node);
		}

		protected void appendKeyData(ArrayNode node) {
		}
	}
}
