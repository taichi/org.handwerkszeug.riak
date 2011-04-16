package org.handwerkszeug.riak.op.mapreduce;

import org.codehaus.jackson.node.ObjectNode;

public abstract class FunctionPhase extends MapReducePhase {

	protected String language;

	public FunctionPhase(PhaseType phase, String language) {
		super(phase);
		this.language = language;
	}

	@Override
	protected void appendPhase(ObjectNode json) {
		json.put("language", this.language);
		appendFunction(json);
	}

	protected abstract void appendFunction(ObjectNode json);
}
