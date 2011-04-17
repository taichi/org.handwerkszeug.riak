package org.handwerkszeug.riak.op.mapreduce;

import org.codehaus.jackson.node.ObjectNode;

public class ErlangPhase extends FunctionPhase {

	protected final String module;

	protected final String function;

	protected ErlangPhase(PhaseType phase, String module, String function) {
		super(phase, "erlang");
		this.module = module;
		this.function = function;
	}

	@Override
	protected void appendFunction(ObjectNode json) {
		json.put("module", this.module);
		json.put("function", this.function);
	}
}