package org.handwerkszeug.riak.http;

public class LinkCondition {

	public static final String ANY = "_";

	final String bucket;

	final String tag;

	final boolean keep;

	public LinkCondition(String bucket, String tag, boolean keep) {
		if (bucket == null) {
			this.bucket = ANY;
		} else {
			this.bucket = bucket;
		}
		if (tag == null) {
			this.tag = ANY;
		} else {
			this.tag = tag;
		}
		this.keep = keep;
	}

	public static LinkCondition bucket(String bucket) {
		return bucket(bucket, false);
	}

	public static LinkCondition bucket(String bucket, boolean keep) {
		return new LinkCondition(bucket, ANY, keep);
	}

	public static LinkCondition tag(String tag) {
		return tag(tag, false);
	}

	public static LinkCondition tag(String tag, boolean keep) {
		return new LinkCondition(ANY, tag, keep);
	}

	public String getBucket() {
		return this.bucket;
	}

	public String getTag() {
		return this.tag;
	}

	public boolean getKeep() {
		return this.keep;
	}
}
