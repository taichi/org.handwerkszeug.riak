package org.handwerkszeug.riak.mapreduce;

import java.util.Arrays;

import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.model.Erlang;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.util.JsonAppender;

/**
 * @author taichi
 */
public abstract class MapReduceInputs implements JsonAppender<ObjectNode> {

	static final String FIELD_INPUTS = "inputs";

	/**
	 * all of the keys in that bucket as inputs
	 * 
	 * @param bucket
	 */
	public static MapReduceInputs bucket(final String bucket) {
		return new MapReduceInputs() {
			@Override
			public void appendTo(ObjectNode json) {
				json.put(FIELD_INPUTS, bucket);
			}
		};
	}

	public static LocationInputs location(Location location) {
		return new LocationInputs(location);
	}

	public static LocationInputs location(Location location,
			final Object keydata) {
		return new LocationInputs(location) {
			@Override
			protected void appendKeyData(ArrayNode node) {
				node.addPOJO(keydata);
			}
		};
	}

	public static LocationInputs location(Location location,
			final String keydata) {
		return new LocationInputs(location) {
			@Override
			protected void appendKeyData(ArrayNode node) {
				node.add(keydata);
			}
		};
	}

	public static LocationInputs location(Location location, final int keydata) {
		return new LocationInputs(location) {
			@Override
			protected void appendKeyData(ArrayNode node) {
				node.add(keydata);
			}
		};
	}

	public static LocationInputs location(Location location, final long keydata) {
		return new LocationInputs(location) {
			@Override
			protected void appendKeyData(ArrayNode node) {
				node.add(keydata);
			}
		};
	}

	public static LocationInputs location(Location location,
			final double keydata) {
		return new LocationInputs(location) {
			@Override
			protected void appendKeyData(ArrayNode node) {
				node.add(keydata);
			}
		};
	}

	public static LocationInputs location(Location location, final float keydata) {
		return new LocationInputs(location) {
			@Override
			protected void appendKeyData(ArrayNode node) {
				node.add(keydata);
			}
		};
	}

	public static MapReduceInputs keyFilter(String bucket,
			MapReduceKeyFilter... keyFilters) {
		return keyFilter(bucket, Arrays.asList(keyFilters));
	}

	public static MapReduceInputs keyFilter(final String bucket,
			final Iterable<MapReduceKeyFilter> keyFilters) {
		return new MapReduceInputs() {
			@Override
			public void appendTo(ObjectNode json) {
				ObjectNode node = json.putObject(FIELD_INPUTS);
				node.put("bucket", bucket);
				ArrayNode array = node.putArray("key_filters");
				for (MapReduceKeyFilter kf : keyFilters) {
					kf.appendTo(array);
				}
			}
		};
	}

	static final Erlang riakSearch = new Erlang("riak_search", "mapred_search");

	/**
	 * @see <a
	 *      href="http://wiki.basho.com/Riak-Search---Querying.html#Querying-Integrated-with-Map-Reduce">Querying
	 *      Integrated with Map/Reduce </a>
	 * @see <a
	 *      href="https://github.com/basho/riak_search/blob/master/apps/riak_search/src/riak_search.erl">riak_search.erl</a>
	 */
	public static MapReduceInputs search(final String index, final String query) {
		return new MapReduceInputs() {
			@Override
			public void appendTo(ObjectNode json) {
				ObjectNode node = json.putObject(FIELD_INPUTS);
				riakSearch.appendTo(node);
				ArrayNode arg = node.putArray("arg");
				arg.add(index);
				arg.add(query);
			}
		};
	}

	static class LocationInputs extends MapReduceInputs {
		final Location location;

		public LocationInputs(Location location) {
			this.location = location;
		}

		@Override
		public void appendTo(ObjectNode json) {
			appendTo(json.putArray(FIELD_INPUTS));
		}

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
