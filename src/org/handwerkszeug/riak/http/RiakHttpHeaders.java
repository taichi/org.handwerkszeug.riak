package org.handwerkszeug.riak.http;

import static org.handwerkszeug.riak.util.Validation.notNull;

public class RiakHttpHeaders {

	public static final String VECTOR_CLOCK = "X-Riak-Vclock";

	public static final String LINK = "Link";

	public static final String USERMETA_PREFIX = "x-riak-meta-";

	public static final String CONTENT_JSON = "application/json";

	public static String toUsermeta(String key) {
		notNull(key, "key");
		return USERMETA_PREFIX + key;
	}
}
