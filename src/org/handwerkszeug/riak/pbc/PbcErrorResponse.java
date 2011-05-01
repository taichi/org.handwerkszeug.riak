package org.handwerkszeug.riak.pbc;

import org.handwerkszeug.riak.model.internal.AbstractRiakResponse;

/**
 * @author taichi
 */
public abstract class PbcErrorResponse<T> extends AbstractRiakResponse {

	final Riakclient.RpbErrorResp error;

	public PbcErrorResponse(Riakclient.RpbErrorResp error) {
		this.error = error;
	}

	@Override
	public int getResponseCode() {
		return this.error.getErrcode();
	}

	@Override
	public String getMessage() {
		return this.error.getErrmsg().toStringUtf8();
	}

}
