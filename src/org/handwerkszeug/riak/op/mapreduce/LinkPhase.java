package org.handwerkszeug.riak.op.mapreduce;

import org.codehaus.jackson.node.ObjectNode;

/**
 * @author taichi
 */
public class LinkPhase extends MapReducePhase {

	protected String bucket;

	protected String tag;

	public LinkPhase(String bucket) {
		this(bucket, null, false);
	}

	public LinkPhase(String bucket, boolean keep) {
		this(bucket, null, keep);
	}

	public LinkPhase(String bucket, String tag) {
		this(bucket, tag, false);
	}

	public LinkPhase(String bucket, String tag, boolean keep) {
		super(PhaseType.link, false);
		this.bucket = bucket;
		this.tag = tag;
	}

	@Override
	public void appendPhase(ObjectNode json) {
		json.put("bucket", this.bucket);
		if (tag != null && tag.isEmpty() == false) {
			json.put("tag", this.tag);
		}
	}
}