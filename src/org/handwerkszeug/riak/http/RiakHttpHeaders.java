package org.handwerkszeug.riak.http;

import static org.handwerkszeug.riak.util.Validation.notNull;

/**
 * @author taichi
 */
public class RiakHttpHeaders {

	public static final String CLIENT_ID = "X-Riak-ClientId";

	public static final String VECTOR_CLOCK = "X-Riak-Vclock";

	public static final String LINK = "Link";

	public static final String USERMETA_PREFIX = "x-riak-meta-";

	public static final String BLOCK_SIZE = "X-Luwak-Block-Size";

	public static final String CONTENT_JSON = "application/json";

	public static final String CONTENT_STREAM = "application/octet-stream";

	public static final String MULTI_PART = "multipart/mixed";

	public static String toUsermeta(String key) {
		notNull(key, "key");
		return USERMETA_PREFIX + key;
	}

	public static boolean isUsermeta(String name) {
		notNull(name, "name");
		return name.startsWith(USERMETA_PREFIX);
	}

	static final int length = USERMETA_PREFIX.length();

	public static String fromUsermeta(String name) {
		return name.substring(length);
	}

}