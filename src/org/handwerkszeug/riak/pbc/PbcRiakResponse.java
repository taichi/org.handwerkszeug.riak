package org.handwerkszeug.riak.pbc;

import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.op.RiakResponse;

public abstract class PbcRiakResponse<T> implements RiakResponse<T> {

	@Override
	public boolean isErrorResponse() {
		return false;
	}

	@Override
	public int getErrorCode() {
		return 0;
	}

	@Override
	public String getErrorMessage() {
		return "";
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
		public int getErrorCode() {
			return this.error.getErrcode();
		}

		@Override
		public String getErrorMessage() {
			return this.error.getErrmsg().toStringUtf8();
		}

		@Override
		public _ getResponse() {
			return _._;
		}
	}
}
