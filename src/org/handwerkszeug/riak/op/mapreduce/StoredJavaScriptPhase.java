package org.handwerkszeug.riak.op.mapreduce;

import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.model.Location;

/**
 * @author taichi
 */
public class StoredJavaScriptPhase extends JavaScriptPhase {

	protected Location location;

	protected StoredJavaScriptPhase(PhaseType phase, Location location,
			Object arg, boolean keep) {
		super(phase, arg, keep);
		this.location = location;
	}

	public static MapReducePhase map(Location location) {
		return new StoredJavaScriptPhase(PhaseType.map, location, null, false);
	}

	public static MapReducePhase map(Location location, Object arg) {
		return new StoredJavaScriptPhase(PhaseType.map, location, arg, false);
	}

	public static MapReducePhase map(Location location, Object arg, boolean keep) {
		return new StoredJavaScriptPhase(PhaseType.map, location, arg, keep);
	}

	public static MapReducePhase reduce(Location location) {
		return new StoredJavaScriptPhase(PhaseType.reduce, location, null,
				false);
	}

	public static MapReducePhase reduce(Location location, boolean keep) {
		return new StoredJavaScriptPhase(PhaseType.reduce, location, null, keep);
	}

	@Override
	protected void appendFunction(ObjectNode json) {
		json.put("bucket", location.getBucket());
		json.put("key", location.getKey());
	}
}