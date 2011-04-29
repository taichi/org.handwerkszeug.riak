package org.handwerkszeug.riak.model;

import java.util.Date;

/**
 * @author taichi
 */
public class DefaultGetOptions implements GetOptions {

	protected DefaultGetOptions() {
	}

	@Override
	public Quorum getReadQuorum() {
		return Quorum.Default;
	}

	@Override
	public String getIfNoneMatch() {
		return null;
	}

	@Override
	public String getIfMatch() {
		return null;
	}

	@Override
	public Date getIfModifiedSince() {
		return null;
	}

}
