package org.handwerkszeug.riak.ease.internal;

import org.handwerkszeug.riak.ease.ExceptionHandler;
import org.handwerkszeug.riak.ease.RiakCommand;
import org.handwerkszeug.riak.model.RiakResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author taichi
 */
public class DefaultExceptionHandler implements ExceptionHandler {

	public static final ExceptionHandler INSTANCE = new DefaultExceptionHandler();

	static final Logger LOG = LoggerFactory
			.getLogger(DefaultExceptionHandler.class);

	@Override
	public void handle(RiakCommand<?> cmd, RiakResponse response) {
		LOG.error(
				"{} {} {}",
				new Object[] { cmd.getClass().getName(),
						response.getResponseCode(), response.getMessage() });
	}

}
