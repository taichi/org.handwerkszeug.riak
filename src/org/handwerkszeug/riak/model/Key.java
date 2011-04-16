package org.handwerkszeug.riak.model;

public class Key {

	final String bucket;
	final String key;

	public Key(String bucket, String key) {
		this.bucket = bucket;
		this.key = key;
	}

	public String getBucket() {
		return this.bucket;
	}

	public String getKey() {
		return this.key;
	}
}
