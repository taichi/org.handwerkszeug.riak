package org.handwerkszeug.riak.pbc;

import org.handwerkszeug.riak.model.internal.AbstractRiakResponse;

/**
 * @author taichi
 */
public class PbcErrorResponse<T> extends AbstractRiakResponse<T> {

	final Riakclient.RpbErrorResp error;

	public PbcErrorResponse(Riakclient.RpbErrorResp error) {
		this.error = error;
	}

	@Override
	public boolean isErrorResponse() {
		return true;
	}

	@Override
	public int getResponseCode() {
		return this.error.getErrcode();
	}

	@Override
	public String getMessage() {
		return this.error.getErrmsg().toStringUtf8();
	}

	@Override
	public T getResponse() {
		throw new UnsupportedOperationException();
	}
}
