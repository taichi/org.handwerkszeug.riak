package org.handwerkszeug.riak.http;

import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * @author taichi
 */
public interface StreamResponseHandler extends
		RiakResponseHandler<ChannelBuffer> {

	/**
	 * at the beginning of streaming.
	 */
	void begin(RiakObject<_> headers) throws Exception;

	/**
	 * at the end of streaming.
	 */
	void end() throws Exception;
}
