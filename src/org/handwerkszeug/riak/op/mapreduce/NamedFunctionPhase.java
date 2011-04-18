package org.handwerkszeug.riak.op.mapreduce;

import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.model.Function;

public class NamedFunctionPhase extends MapReducePhase {

	final Function function;

	public NamedFunctionPhase(PhaseType phase, Function function, boolean keep) {
		super(phase, keep);
		this.function = function;
	}

	public static MapReducePhase map(Function function) {
		return new NamedFunctionPhase(PhaseType.map, function, false);
	}

	public static MapReducePhase map(Function function, boolean keep) {
		return new NamedFunctionPhase(PhaseType.map, function, keep);
	}

	@Override
	protected void appendPhase(ObjectNode json) {
		json.put("language", this.function.getLanguage());
		this.function.appendTo(json);
	}

}
