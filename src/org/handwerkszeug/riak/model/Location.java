package org.handwerkszeug.riak.model;

/**
 * @author taichi
 */
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

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Location [bucket=");
		builder.append(this.bucket);
		builder.append(", key=");
		builder.append(this.key);
		builder.append("]");
		return builder.toString();
	}

}
