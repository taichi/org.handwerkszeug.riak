package org.handwerkszeug.riak.http.rest.internal;

import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.handwerkszeug.riak.op.internal.CompletionSupport;
import org.handwerkszeug.riak.util.NettyUtil;
import org.handwerkszeug.riak.util.NettyUtil.MessageHandler;
import org.jboss.netty.handler.codec.http.HttpResponse;

/**
 * @author taichi
 */
public class ContinuousMessageHandler<T> implements NettyUtil.MessageHandler {
	final CompletionSupport support;
	final RiakResponseHandler<T> users;
	final MessageHandler internal;

	public ContinuousMessageHandler(RiakResponseHandler<T> users,
			MessageHandler internal, CompletionSupport support) {
		this.users = users;
		this.internal = internal;
		this.support = support;
	}

	@Override
	public boolean handle(Object receive) throws Exception {
		if (receive instanceof HttpResponse) {
			HttpResponse response = (HttpResponse) receive;
			if (NettyUtil.isError(response.getStatus())) {
				users.onError(new RestErrorResponse(response, this.support));
				return true;
			}
		}
		return internal.handle(receive);
	}
}