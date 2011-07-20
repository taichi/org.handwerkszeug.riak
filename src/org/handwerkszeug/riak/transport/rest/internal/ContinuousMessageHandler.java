package org.handwerkszeug.riak.transport.rest.internal;

import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.handwerkszeug.riak.transport.internal.CompletionSupport;
import org.handwerkszeug.riak.transport.internal.CountDownRiakFuture;
import org.handwerkszeug.riak.transport.internal.MessageHandler;
import org.handwerkszeug.riak.util.NettyUtil;
import org.jboss.netty.handler.codec.http.HttpResponse;

/**
 * @author taichi
 */
public class ContinuousMessageHandler<T> implements MessageHandler {
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
	public boolean handle(Object receive, CountDownRiakFuture future)
			throws Exception {
		if (receive instanceof HttpResponse) {
			HttpResponse response = (HttpResponse) receive;
			if (NettyUtil.isError(response.getStatus())) {
				this.users
						.onError(new RestErrorResponse(response));
				future.setFailure();
				return true;
			}
		}
		return this.internal.handle(receive, future);
	}
}