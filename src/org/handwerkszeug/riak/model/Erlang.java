package org.handwerkszeug.riak.model;

import org.codehaus.jackson.node.ObjectNode;

/**
 * JSON node key is different between M/R and pre/post commit operation.
 * 
 * @author taichi
 */
public class Erlang implements Function {

	final String module;
	final String function;

	public Erlang(String module, String function) {
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

	// TODO list built-in functions
	public static final Erlang map_object_value = new Erlang(
			"riak_kv_mapreduce", "map_object_value");

	public static final Erlang reduce_sum = new Erlang("riak_kv_mapreduce",
			"reduce_sum");
}
