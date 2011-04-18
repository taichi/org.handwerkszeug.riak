package org.handwerkszeug.riak.mapreduce;

import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.model.Location;

/**
 * @author taichi
 */
public abstract class JavaScriptPhase extends MapReducePhase {

	protected JavaScriptPhase(PhaseType phase, boolean keep) {
		super(phase, keep);
	}

	@Override
	protected void appendPhase(ObjectNode json) {
		json.put("language", "javascript");
		appendFunction(json);
	}

	protected abstract void appendFunction(ObjectNode json);

	/**
	 * Map phases may also be passed static arguments by using the “arg” spec
	 * field.
	 */
	protected void appendArg(ObjectNode json, Object arg) {
		json.putPOJO("arg", arg);
	}

	/**
	 * if you want to built-in javascript function, use
	 * {@link NamedFunctionPhase}.
	 * 
	 * @see NamedFunctionPhase
	 */
	public static class AdHoc extends JavaScriptPhase {

		protected String source;

		protected AdHoc(PhaseType phase, String source, boolean keep) {
			super(phase, keep);
			this.source = source;
		}

		public static MapReducePhase map(String source) {
			return new AdHoc(PhaseType.map, source, false);
		}

		public static MapReducePhase map(String source, final Object arg) {
			return map(source, arg);
		}

		public static MapReducePhase map(String source, final Object arg,
				boolean keep) {
			return new AdHoc(PhaseType.map, source, keep) {
				@Override
				protected void appendFunction(ObjectNode json) {
					appendSource(json);
					appendArg(json, arg);
				}
			};
		}

		public MapReducePhase reduce(String source) {
			return new AdHoc(PhaseType.reduce, source, false);
		}

		public MapReducePhase reduce(String source, boolean keep) {
			return new AdHoc(PhaseType.reduce, source, keep);
		}

		@Override
		protected void appendFunction(ObjectNode json) {
			appendSource(json);
		}

		protected void appendSource(ObjectNode json) {
			json.put("source", this.source);
		}
	}

	public static class Stored extends JavaScriptPhase {

		protected Location location;

		protected Stored(PhaseType phase, Location location, boolean keep) {
			super(phase, keep);
			this.location = location;
		}

		public static MapReducePhase map(Location location) {
			return map(location, false);
		}

		public static MapReducePhase map(Location location, boolean keep) {
			return new Stored(PhaseType.map, location, keep);
		}

		public static MapReducePhase map(Location location, Object arg) {
			return map(location, arg, false);
		}

		public static MapReducePhase map(Location location, final Object arg,
				boolean keep) {
			return new Stored(PhaseType.map, location, keep) {
				@Override
				protected void appendFunction(ObjectNode json) {
					appendLocation(json);
					appendArg(json, arg);
				}
			};
		}

		public static MapReducePhase reduce(Location location) {
			return reduce(location, false);
		}

		public static MapReducePhase reduce(Location location, boolean keep) {
			return new Stored(PhaseType.reduce, location, keep);
		}

		@Override
		protected void appendFunction(ObjectNode json) {
			appendLocation(json);
		}

		protected void appendLocation(ObjectNode json) {
			json.put("bucket", this.location.getBucket());
			json.put("key", this.location.getKey());
		}
	}
}
