package org.handwerkszeug.riak.model;

public class Erlang {

	final String module;
	final String function;

	public Erlang(String module, String function) {
		this.module = module;
		this.function = function;
	}

	public String getModule() {
		return this.module;
	}

	public String getFunction() {
		return this.function;
	}
}
