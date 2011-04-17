package org.handwerkszeug.riak.op.mapreduce;

import org.codehaus.jackson.node.ObjectNode;

/**
 * @author taichi
 */
public class AdHocJavaScriptPhase extends JavaScriptPhase {

	enum BuiltInType {
		/**
		 * {jsfun,Name} where Name is a binary that, when evaluated in
		 * Javascript, points to a built-in Javascript function.
		 */
		named() {
			@Override
			void appendFunctionBody(ObjectNode json, String source) {
				json.put("name", source);
			}
		},
		anonymous() {
			@Override
			void appendFunctionBody(ObjectNode json, String source) {
				json.put("source", source);
			}
		};

		abstract void appendFunctionBody(ObjectNode json, String source);
	}

	protected BuiltInType builtIn;
	protected String source;

	protected AdHocJavaScriptPhase(PhaseType phase, BuiltInType type, String source,
			Object arg) {
		super(phase, arg);
		this.source = source;
	}

	public static JavaScriptPhase map(String source) {
		return new AdHocJavaScriptPhase(PhaseType.map, BuiltInType.anonymous,
				source, null);
	}

	public static JavaScriptPhase map(String source, Object arg) {
		return new AdHocJavaScriptPhase(PhaseType.map, BuiltInType.anonymous,
				source, arg);
	}

	public JavaScriptPhase reduce(String source) {
		return new AdHocJavaScriptPhase(PhaseType.reduce, BuiltInType.anonymous,
				source, null);
	}

	public static JavaScriptPhase mapByBuiltIn(String name) {
		return new AdHocJavaScriptPhase(PhaseType.map, BuiltInType.named, name, null);
	}

	public static JavaScriptPhase mapByBuiltIn(String name, Object arg) {
		return new AdHocJavaScriptPhase(PhaseType.map, BuiltInType.named, name, arg);
	}

	public JavaScriptPhase reduceByBuiltIn(String name) {
		return new AdHocJavaScriptPhase(PhaseType.reduce, BuiltInType.named, name,
				null);
	}

	@Override
	protected void appendFunctionBody(ObjectNode json) {
		this.builtIn.appendFunctionBody(json, this.source);
	}
}
