package org.handwerkszeug.riak.op.mapreduce;

import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.POJONode;

public abstract class JavaScriptPhase extends FunctionPhase {

	/**
	 * Map phases may also be passed static arguments by using the “arg” spec
	 * field.
	 */
	protected Object arg;

	public JavaScriptPhase(PhaseType phase) {
		super(phase, "javascript");
	}

	public JavaScriptPhase(PhaseType phase, Object arg) {
		this(phase);
		this.arg = arg;
	}

	@Override
	protected void appendFunction(ObjectNode json) {
		appendFunctionBody(json);
		if (arg != null) {
			json.put("arg", new POJONode(this.arg));
		}
	}

	protected abstract void appendFunctionBody(ObjectNode json);

}
