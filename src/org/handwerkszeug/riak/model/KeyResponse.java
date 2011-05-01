package org.handwerkszeug.riak.model;

import java.util.List;

/**
 * @author taichi
 */
public class KeyResponse {

	final List<String> keys;
	final boolean done;

	public KeyResponse(List<String> keys, boolean done) {
		this.keys = keys;
		this.done = done;
	}

	public List<String> getKeys() {
		return this.keys;
	}

	public boolean getDone() {
		return this.done;
	}
}
