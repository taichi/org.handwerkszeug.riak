package org.handwerkszeug.riak.util;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;

/**
 * @author taichi
 */
public class NettyUtil {

	public static boolean isError(HttpResponseStatus status) {
		int i = status.getCode();
		return 400 <= i && i <= 599;
	}

	public static boolean isSuccessful(HttpResponseStatus status) {
		int i = status.getCode();
		return 200 <= i && i <= 299;
	}
}
