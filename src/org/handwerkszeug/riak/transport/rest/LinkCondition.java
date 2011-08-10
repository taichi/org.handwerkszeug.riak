package org.handwerkszeug.riak.transport.rest;

/**
 * @author taichi
 */
public class LinkCondition {

	public static final String WILDCARD = "_";

	final String bucket;

	final String tag;

	final boolean keep;

	public LinkCondition(String bucket, String tag, boolean keep) {
		if (bucket == null) {
			this.bucket = WILDCARD;
		} else {
			this.bucket = bucket;
		}
		if (tag == null) {
			this.tag = WILDCARD;
		} else {
			this.tag = tag;
		}
		this.keep = keep;
	}

	public static final LinkCondition ANY = new LinkCondition(WILDCARD,
			WILDCARD, false);

	public static final LinkCondition KEEP_ANY = new LinkCondition(WILDCARD,
			WILDCARD, true);

	public static LinkCondition bucket(String bucket) {
		return bucket(bucket, false);
	}

	public static LinkCondition bucket(String bucket, boolean keep) {
		return new LinkCondition(bucket, WILDCARD, keep);
	}

	public static LinkCondition tag(String tag) {
		return tag(tag, false);
	}

	public static LinkCondition tag(String tag, boolean keep) {
		return new LinkCondition(WILDCARD, tag, keep);
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
