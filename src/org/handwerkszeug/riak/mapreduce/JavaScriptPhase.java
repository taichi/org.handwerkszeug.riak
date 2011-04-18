package org.handwerkszeug.riak.mapreduce;

import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.model.JavaScript;
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
	 * if you want to built-in javascript function, use {@link BuiltIn}.
	 * 
	 * @see BuiltIn
	 */
	public static class AdHoc extends JavaScriptPhase {

		protected String source;

		protected AdHoc(PhaseType phase, String source, boolean keep) {
			super(phase, keep);
			this.source = source;
		}

		public static MapReducePhase map(String source) {
			return map(source, false);
		}

		public static MapReducePhase map(String source, boolean keep) {
			return new AdHoc(PhaseType.map, source, keep);
		}

		public static MapReducePhase map(String source, Object arg) {
			return map(source, arg);
		}

		public static MapReducePhase map(String source, Object arg, boolean keep) {
			return withArg(PhaseType.map, source, arg, keep);
		}

		static MapReducePhase withArg(PhaseType type, String source,
				final Object arg, boolean keep) {
			return new AdHoc(type, source, keep) {
				@Override
				protected void appendFunction(ObjectNode json) {
					appendSource(json);
					appendArg(json, arg);
				}
			};
		}

		public MapReducePhase reduce(String source) {
			return reduce(source, false);
		}

		public MapReducePhase reduce(String source, boolean keep) {
			return new AdHoc(PhaseType.reduce, source, keep);
		}

		public static MapReducePhase reduce(String source, Object arg) {
			return withArg(PhaseType.reduce, source, arg, false);
		}

		public static MapReducePhase reduce(String source, Object arg,
				boolean keep) {
			return withArg(PhaseType.reduce, source, arg, keep);
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

		public static MapReducePhase map(Location location, Object arg,
				boolean keep) {
			return withArg(PhaseType.map, location, arg, keep);
		}

		static MapReducePhase withArg(PhaseType type, Location location,
				final Object arg, boolean keep) {
			return new Stored(type, location, keep) {
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

		public static MapReducePhase reduce(Location location, Object arg) {
			return withArg(PhaseType.map, location, arg, false);
		}

		public static MapReducePhase reduce(Location location, Object arg,
				boolean keep) {
			return withArg(PhaseType.map, location, arg, keep);
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

	/**
	 * @see <a
	 *      href="https://github.com/basho/riak_kv/blob/master/priv/mapred_builtins.js">mapred_builtins.js</a>
	 */
	public static class BuiltIn extends JavaScriptPhase {
		final JavaScript builtIn;

		public BuiltIn(PhaseType type, JavaScript builtIn, boolean keep) {
			super(type, keep);
			this.builtIn = builtIn;
		}

		public static MapReducePhase map(JavaScript builtIn) {
			return map(builtIn, false);
		}

		public static MapReducePhase map(JavaScript builtIn, boolean keep) {
			return new BuiltIn(PhaseType.map, builtIn, keep);
		}

		public static MapReducePhase map(JavaScript builtIn, Object arg) {
			return withArg(PhaseType.map, builtIn, arg, false);
		}

		public static MapReducePhase map(JavaScript builtIn, Object arg,
				boolean keep) {
			return withArg(PhaseType.map, builtIn, arg, keep);
		}

		static MapReducePhase withArg(PhaseType type, JavaScript builtIn,
				final Object arg, boolean keep) {
			return new BuiltIn(type, builtIn, keep) {
				@Override
				protected void appendFunction(ObjectNode json) {
					this.builtIn.appendTo(json);
					appendArg(json, arg);
				}
			};
		}

		public static MapReducePhase reduce(JavaScript builtIn) {
			return reduce(builtIn, false);
		}

		public static MapReducePhase reduce(JavaScript builtIn, boolean keep) {
			return new BuiltIn(PhaseType.reduce, builtIn, keep);
		}

		public static MapReducePhase reduce(JavaScript builtIn, Object arg) {
			return withArg(PhaseType.reduce, builtIn, arg, false);
		}

		public static MapReducePhase reduce(JavaScript builtIn, Object arg,
				boolean keep) {
			return withArg(PhaseType.reduce, builtIn, arg, keep);
		}

		@Override
		protected void appendFunction(ObjectNode json) {
			this.builtIn.appendTo(json);
		}
	}
}
