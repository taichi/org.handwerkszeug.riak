package org.handwerkszeug.riak.model;

import static org.handwerkszeug.riak.util.Validation.notNull;

/**
 * @author taichi
 */
public class Location {

	final String bucket;
	final String key;

	public Location(String bucket, String key) {
		notNull(bucket, "bucket");
		notNull(key, "key");
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
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + bucket.hashCode();
		result = prime * result + key.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		return equals((Location) obj);
	}

	public boolean equals(Location other) {
		return this.bucket.equals(other.getBucket())
				&& this.key.equals(other.getKey());
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
