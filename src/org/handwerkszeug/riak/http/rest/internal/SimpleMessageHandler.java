package org.handwerkszeug.riak.http.rest.internal;

import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.handwerkszeug.riak.op.internal.CompletionSupport;
import org.handwerkszeug.riak.op.internal.IncomprehensibleProtocolException;
import org.handwerkszeug.riak.util.NettyUtil;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

/**
 * @author taichi
 */
public class SimpleMessageHandler implements NettyUtil.MessageHandler {
	final String name;
	final RiakResponseHandler<_> users;
	final CompletionSupport support;

	public SimpleMessageHandler(String name, RiakResponseHandler<_> users,
			CompletionSupport support) {
		this.users = users;
		this.name = name;
		this.support = support;
	}

	@Override
	public boolean handle(Object receive) throws Exception {
		if (receive instanceof HttpResponse) {
			HttpResponse response = (HttpResponse) receive;
			HttpResponseStatus status = response.getStatus();
			if (NettyUtil.isError(status)) {
				users.onError(new RestErrorResponse(response, support));
				return true;
			}
			if (NettyUtil.isSuccessful(status)) {
				users.handle(support.newResponse());
				return true;
			}
		}
		throw new IncomprehensibleProtocolException(name);
	}
}