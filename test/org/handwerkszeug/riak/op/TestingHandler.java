package org.handwerkszeug.riak.op;

import static org.junit.Assert.fail;

import org.handwerkszeug.riak.model.RiakResponse;

/**
 * @author taichi
 */
public abstract class TestingHandler<T> implements RiakResponseHandler<T> {

	@Override
	public void onError(RiakResponse response) throws Exception {
		fail(response.getMessage());
	}

}
