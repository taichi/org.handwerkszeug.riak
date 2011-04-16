package org.handwerkszeug.riak.op.mapreduce;

import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.model.Key;

/**
 * @author taichi
 */
public class StoredJavaScript extends JavaScriptFunction {

	protected Key key;

	protected StoredJavaScript(PhaseType phase, Key key, Object arg) {
		super(phase, arg);
		this.key = key;
	}

	public static JavaScriptFunction map(Key key) {
		return new StoredJavaScript(PhaseType.map, key, null);
	}

	public static JavaScriptFunction map(Key key, Object arg) {
		return new StoredJavaScript(PhaseType.map, key, arg);
	}

	public static JavaScriptFunction reduce(Key key) {
		return new StoredJavaScript(PhaseType.reduce, key, null);
	}

	@Override
	protected void appendFunctionBody(ObjectNode json) {
		json.put("bucket", key.getBucket());
		json.put("key", key.getKey());
	}
}