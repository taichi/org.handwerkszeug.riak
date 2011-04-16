package org.handwerkszeug.riak.op.mapreduce;

import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.model.Location;

/**
 * @author taichi
 */
public class StoredJavaScript extends JavaScriptFunction {

	protected Location key;

	protected StoredJavaScript(PhaseType phase, Location key, Object arg) {
		super(phase, arg);
		this.key = key;
	}

	public static JavaScriptFunction map(Location key) {
		return new StoredJavaScript(PhaseType.map, key, null);
	}

	public static JavaScriptFunction map(Location key, Object arg) {
		return new StoredJavaScript(PhaseType.map, key, arg);
	}

	public static JavaScriptFunction reduce(Location key) {
		return new StoredJavaScript(PhaseType.reduce, key, null);
	}

	@Override
	protected void appendFunctionBody(ObjectNode json) {
		json.put("bucket", key.getBucket());
		json.put("key", key.getKey());
	}
}