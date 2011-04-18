package org.handwerkszeug.riak.mapreduce;

import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.model.Location;

/**
 * @author taichi
 */
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
			json.putPOJO("arg", this.arg);
		}
	}

	protected abstract void appendFunction(ObjectNode json);

	/**
	 * if you want to built-in javascript function, use
	 * {@link NamedFunctionPhase}.
	 * 
	 * @see NamedFunctionPhase
	 */
	public static class AdHoc extends JavaScriptPhase {

		protected String source;

		protected AdHoc(PhaseType phase, String source, Object arg, boolean keep) {
			super(phase, arg, keep);
			this.source = source;
		}

		public static MapReducePhase map(String source) {
			return new AdHoc(PhaseType.map, source, null, false);
		}

		public static MapReducePhase map(String source, Object arg) {
			return new AdHoc(PhaseType.map, source, arg, false);
		}

		public static MapReducePhase map(String source, Object arg, boolean keep) {
			return new AdHoc(PhaseType.map, source, arg, keep);
		}

		public MapReducePhase reduce(String source) {
			return new AdHoc(PhaseType.reduce, source, null, false);
		}

		public MapReducePhase reduce(String source, boolean keep) {
			return new AdHoc(PhaseType.reduce, source, null, keep);
		}

		@Override
		protected void appendFunction(ObjectNode json) {
			json.put("source", this.source);
		}
	}

	public static class Stored extends JavaScriptPhase {

		protected Location location;

		protected Stored(PhaseType phase, Location location, Object arg,
				boolean keep) {
			super(phase, arg, keep);
			this.location = location;
		}

		public static MapReducePhase map(Location location) {
			return new Stored(PhaseType.map, location, null, false);
		}

		public static MapReducePhase map(Location location, Object arg) {
			return new Stored(PhaseType.map, location, arg, false);
		}

		public static MapReducePhase map(Location location, Object arg,
				boolean keep) {
			return new Stored(PhaseType.map, location, arg, keep);
		}

		public static MapReducePhase reduce(Location location) {
			return new Stored(PhaseType.reduce, location, null, false);
		}

		public static MapReducePhase reduce(Location location, boolean keep) {
			return new Stored(PhaseType.reduce, location, null, keep);
		}

		@Override
		protected void appendFunction(ObjectNode json) {
			json.put("bucket", location.getBucket());
			json.put("key", location.getKey());
		}
	}
}
