package org.handwerkszeug.riak.model;

import static org.handwerkszeug.riak.util.Validation.notNull;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerator;

public abstract class Erlang extends Function {

	protected String module;
	protected String function;

	protected Erlang(String module, String function) {
		super(Language.erlang);
		this.module = module;
		this.function = function;
	}

	public String getModule() {
		return this.module;
	}

	public String getFunction() {
		return this.function;
	}

	public static Erlang newFunction(String module, final String function) {
		notNull(module, "module");
		notNull(function, "function");
		return new Erlang(module, function) {
			@Override
			protected void appendBody(JsonGenerator generator)
					throws IOException {
				// TODO pre/post commit Erlang function
				// json.put("mod", this.getModule());
				// json.put("fun", this.getFunction());
				generator.writeStringField("module", this.module);
				generator.writeStringField("function", this.function);
			}
		};
	}

	public static Erlang riak_kv_mapreduce(String function) {
		return newFunction("riak_kv_mapreduce", function);
	}

	/*
	 * BuiltIn Functions
	 */

	public static final Erlang map_identity = riak_kv_mapreduce("map_identity");
	public static final Erlang map_object_value = riak_kv_mapreduce("map_object_value");
	public static final Erlang map_object_value_list = riak_kv_mapreduce("map_object_value_list");
	public static final Erlang reduce_identity = riak_kv_mapreduce("reduce_identity");
	public static final Erlang reduce_set_union = riak_kv_mapreduce("reduce_set_union");
	public static final Erlang reduce_sort = riak_kv_mapreduce("reduce_sort");
	public static final Erlang reduce_string_to_integer = riak_kv_mapreduce("reduce_string_to_integer");
	public static final Erlang reduce_sum = riak_kv_mapreduce("reduce_sum");
	public static final Erlang reduce_plist_sum = riak_kv_mapreduce("reduce_plist_sum");
	public static final Erlang reduce_count_inputs = riak_kv_mapreduce("reduce_count_inputs");

	public static final Erlang riakSearch = newFunction("riak_search",
			"mapred_search");
}
