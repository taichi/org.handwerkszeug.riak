package org.handwerkszeug.riak.op.mapreduce;

import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.model.Location;

/**
 * @author taichi
 */
public class StoredJavaScriptPhase extends JavaScriptPhase {

	protected Location key;

	protected StoredJavaScriptPhase(PhaseType phase, Location key, Object arg) {
		super(phase, arg);
		this.key = key;
	}

	public static JavaScriptPhase map(Location key) {
		return new StoredJavaScriptPhase(PhaseType.map, key, null);
	}

	public static JavaScriptPhase map(Location key, Object arg) {
		return new StoredJavaScriptPhase(PhaseType.map, key, arg);
	}

	public static JavaScriptPhase reduce(Location key) {
		return new StoredJavaScriptPhase(PhaseType.reduce, key, null);
	}

	@Override
	protected void appendFunctionBody(ObjectNode json) {
		json.put("bucket", key.getBucket());
		json.put("key", key.getKey());
	}
}