package org.handwerkszeug.riak.mapreduce;

import org.codehaus.jackson.node.ObjectNode;

/**
 * @author taichi
 */
public class LinkPhase extends MapReducePhase {

	protected LinkPhase(boolean keep) {
		super(PhaseType.link, keep);
	}

	public static MapReducePhase link() {
		return link(true);
	}

	public static MapReducePhase link(final boolean keep) {
		return new LinkPhase(keep);
	}

	public static MapReducePhase link(String bucket) {
		return link(bucket, true);
	}

	public static MapReducePhase link(final String bucket, boolean keep) {
		return new LinkPhase(keep) {
			@Override
			public void appendPhase(ObjectNode json) {
				json.put("bucket", bucket);
			}
		};
	}

	public static MapReducePhase link(String bucket, String tag) {
		return link(bucket, tag, true);
	}

	public static MapReducePhase link(final String bucket, final String tag,
			boolean keep) {
		return new LinkPhase(keep) {
			@Override
			public void appendPhase(ObjectNode json) {
				json.put("bucket", bucket);
				json.put("tag", tag);
			}
		};
	}

	@Override
	public void appendPhase(ObjectNode json) {
	}
}