package org.handwerkszeug.riak.transport.internal;

import org.handwerkszeug.riak.Markers;
import org.handwerkszeug.riak.nls.Messages;
import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author taichi
 */
public class DefaultCompletionChannelHandler<T> extends
		AbstractCompletionChannelHandler<T> {

	static final Logger LOG = LoggerFactory
			.getLogger(DefaultCompletionChannelHandler.class);

	final MessageHandler handler;

	public DefaultCompletionChannelHandler(CompletionSupport support,
			String name, RiakResponseHandler<T> users, MessageHandler handler,
			CountDownRiakFuture future) {
		super(support, name, users, future);
		this.handler = handler;
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		try {
			Object receive = e.getMessage();
			if (LOG.isDebugEnabled()) {
				LOG.debug(Markers.DETAIL, Messages.Receive, this.name, receive);
			}
			if (this.handler.handle(receive, this.future)) {
				this.support.decrementProgress(this.name);
				this.support.invokeNext();
			}
			e.getFuture().addListener(this.support);
		} catch (Exception ex) {
			throw ex;
		} catch (Error ex) {
			setFailure(ex);
			throw ex;
		}
	}
}