package org.handwerkszeug.riak.transport.rest.internal;

import org.handwerkszeug.riak.model.RiakResponse;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.util.CharsetUtil;

/**
 * @author taichi
 */
public class RestErrorResponse implements RiakResponse {

	final HttpResponse master;

	public RestErrorResponse(HttpResponse master) {
		this.master = master;
	}

	@Override
	public int getResponseCode() {
		return this.master.getStatus().getCode();
	}

	@Override
	public String getMessage() {
		ChannelBuffer content = this.master.getContent();
		if (content.readable()) {
			return content.toString(CharsetUtil.UTF_8);
		}
		return "";
	}

}