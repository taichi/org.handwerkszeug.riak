package org.handwerkszeug.riak.pbc;

import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.op.RiakResponse;

public abstract class PbcRiakResponse<T> implements RiakResponse<T> {

	@Override
	public boolean isErrorResponse() {
		return false;
	}

	@Override
	public int getResponseCode() {
		return 0;
	}

	@Override
	public String getMessage() {
		return "";
	}

	protected static class NoOpResponse extends PbcRiakResponse<_> {
		@Override
		public _ getResponse() {
			return _._;
		}
	}

	protected static class ErrorResponse extends PbcRiakResponse<_> {

		Riakclient.RpbErrorResp error;

		public ErrorResponse(Riakclient.RpbErrorResp error) {
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
		public _ getResponse() {
			return _._;
		}
	}
}
