package org.handwerkszeug.riak.op.mapreduce;

import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.POJONode;

public abstract class JavaScriptPhase extends MapReducePhase {

	/**
	 * Map phases may also be passed static arguments by using the “arg” spec
	 * field.
	 */
	protected Object arg;

	protected JavaScriptPhase(PhaseType phase, Object arg, boolean keep) {
		super(phase, keep);
		this.arg = arg;
	}

	@Override
	protected void appendPhase(ObjectNode json) {
		json.put("language", "javascript");
		appendFunction(json);
		if (arg != null) {
			json.put("arg", new POJONode(this.arg));
		}
	}

	protected abstract void appendFunction(ObjectNode json);

}
