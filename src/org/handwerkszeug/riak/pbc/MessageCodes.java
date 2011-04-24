package org.handwerkszeug.riak.pbc;

public enum MessageCodes {
	RpbErrorResp(0), RpbPingReq(1), RpbPingResp(2);

	private final int code;

	MessageCodes(int code) {
		this.code = code;
	}

	public int code() {
		return this.code;
	}
}
