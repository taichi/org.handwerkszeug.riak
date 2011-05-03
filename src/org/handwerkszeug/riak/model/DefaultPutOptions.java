package org.handwerkszeug.riak.model;

import java.util.Date;

/**
 * @author taichi
 */
public class DefaultPutOptions implements PutOptions {

	protected DefaultPutOptions() {
	}

	@Override
	public String getVectorClock() {
		return null;
	}

	@Override
	public Quorum getReadQuorum() {
		return null;
	}

	@Override
	public Quorum getWriteQuorum() {
		return null;
	}

	@Override
	public Quorum getDurableWriteQuorum() {
		return null;
	}

	@Override
	public boolean getReturnBody() {
		return false;
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

	@Override
	public Date getIfUnmodifiedSince() {
		return null;
	}

}
