package org.handwerkszeug.riak.model;

/**
 * @author taichi
 */
public class DefaultPostOptions implements PostOptions {

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

}
