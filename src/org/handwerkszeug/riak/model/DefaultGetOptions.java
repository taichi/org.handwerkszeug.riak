package org.handwerkszeug.riak.model;

import java.util.Date;

/**
 * @author taichi
 */
public class DefaultGetOptions implements GetOptions {

	public DefaultGetOptions() {
	}

	@Override
	public Quorum getReadQuorum() {
		return null;
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
