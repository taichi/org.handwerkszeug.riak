package org.handwerkszeug.riak.op.mapreduce;

import org.codehaus.jackson.node.ObjectNode;

/**
 * if you want to built-in javascript function, use {@link NamedFunctionPhase}.
 * 
 * @author taichi
 * @see NamedFunctionPhase
 */
public class AdHocJavaScriptPhase extends JavaScriptPhase {

	protected String source;

	protected AdHocJavaScriptPhase(PhaseType phase, String source, Object arg,
			boolean keep) {
		super(phase, arg, keep);
		this.source = source;
	}

	public static MapReducePhase map(String source) {
		return new AdHocJavaScriptPhase(PhaseType.map, source, null, false);
	}

	public static MapReducePhase map(String source, Object arg) {
		return new AdHocJavaScriptPhase(PhaseType.map, source, arg, false);
	}

	public static MapReducePhase map(String source, Object arg, boolean keep) {
		return new AdHocJavaScriptPhase(PhaseType.map, source, arg, keep);
	}

	public MapReducePhase reduce(String source) {
		return new AdHocJavaScriptPhase(PhaseType.reduce, source, null, false);
	}

	public MapReducePhase reduce(String source, boolean keep) {
		return new AdHocJavaScriptPhase(PhaseType.reduce, source, null, keep);
	}

	@Override
	protected void appendFunction(ObjectNode json) {
		json.put("source", this.source);
	}
}
