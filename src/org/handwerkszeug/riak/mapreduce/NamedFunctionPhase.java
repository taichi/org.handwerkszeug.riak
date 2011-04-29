package org.handwerkszeug.riak.mapreduce;

import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.model.Function;

/**
 * @author taichi
 * @see <a
 *      href="https://github.com/basho/riak_kv/blob/master/src/riak_kv_mapreduce.erl">riak_kv_mapreduce.erl</a>
 */
public class NamedFunctionPhase extends MapReducePhase {

	final Function function;

	public NamedFunctionPhase(PhaseType phase, Function function, boolean keep) {
		super(phase, keep);
		this.function = function;
	}

	public static MapReducePhase map(Function function) {
		return map(function, false);
	}

	public static MapReducePhase map(Function function, boolean keep) {
		return new NamedFunctionPhase(PhaseType.map, function, keep);
	}

	public static MapReducePhase reduce(Function function) {
		return reduce(function, false);
	}

	public static MapReducePhase reduce(Function function, boolean keep) {
		return new NamedFunctionPhase(PhaseType.reduce, function, keep);
	}

	@Override
	protected void appendPhase(ObjectNode json) {
		json.put("language", this.function.getLanguage());
		this.function.appendTo(json);
	}

}
