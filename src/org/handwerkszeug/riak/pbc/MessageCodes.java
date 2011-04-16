package org.handwerkszeug.riak.pbc;

public enum MessageCodes {
	RpbErrorResp(1), RpbPingReq(2), RpbPingResp(3);

	private final int code;

	MessageCodes(int code) {
		this.code = code;
	}

	public int code() {
		return this.code;
	}
}
