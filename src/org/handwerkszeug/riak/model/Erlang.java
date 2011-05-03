package org.handwerkszeug.riak.model;

import static org.handwerkszeug.riak.util.Validation.notNull;

import org.codehaus.jackson.node.ObjectNode;

/**
 * JSON node key is different between M/R and pre/post commit operation.
 * 
 * 
 * @author taichi
 * @see <a
 *      href="https://github.com/basho/riak_kv/blob/master/src/riak_kv_mapreduce.erl">riak_kv_mapreduce.erl</a>
 */
public class Erlang implements Function {

	final String module;
	final String function;

	public Erlang(String module, String function) {
		notNull(module, "module");
		notNull(function, "function");
		this.module = module;
		this.function = function;
	}

	@Override
	public String getLanguage() {
		return "erlang";
	}

	public String getModule() {
		return this.module;
	}

	public String getFunction() {
		return this.function;
	}

	@Override
	public void appendTo(ObjectNode json) {
		// TODO pre/post commit Erlang function
		// json.put("mod", this.getModule());
		// json.put("fun", this.getFunction());
		json.put("module", this.getModule());
		json.put("function", this.getFunction());
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Erlang [module=");
		builder.append(module);
		builder.append(", function=");
		builder.append(function);
		builder.append("]");
		return builder.toString();
	}

	public static Erlang riak_kv_mapreduce(String function) {
		return new Erlang("riak_kv_mapreduce", function);
	}

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
}
