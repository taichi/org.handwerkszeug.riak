package org.handwerkszeug.riak.op.mapreduce;

import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.model.Erlang;

public class ErlangPhase extends FunctionPhase {

	final Erlang erlang;

	public ErlangPhase(PhaseType phase, Erlang erlang) {
		super(phase, "erlang");
		this.erlang = erlang;
	}

	@Override
	protected void appendFunction(ObjectNode json) {
		json.put("module", this.erlang.getModule());
		json.put("function", this.erlang.getFunction());
	}
}