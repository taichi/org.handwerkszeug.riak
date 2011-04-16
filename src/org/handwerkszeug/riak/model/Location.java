package org.handwerkszeug.riak.model;

public class Location {

	final String bucket;
	final String key;

	public Location(String bucket, String key) {
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
