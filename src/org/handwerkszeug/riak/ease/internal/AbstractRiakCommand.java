package org.handwerkszeug.riak.ease.internal;

import org.handwerkszeug.riak.RiakClient;
import org.handwerkszeug.riak.ease.ExceptionHandler;
import org.handwerkszeug.riak.ease.RiakCommand;
import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.model.RiakResponse;
import org.handwerkszeug.riak.op.RiakOperations;
import org.handwerkszeug.riak.op.RiakResponseHandler;

/**
 * @author taichi
 * @param <V>
 * @param <OP>
 */
public abstract class AbstractRiakCommand<V, OP extends RiakOperations>
		implements RiakCommand<V> {

	protected final RiakClient<OP> client;
	protected final ExceptionHandler handler;

	protected AbstractRiakCommand(RiakClient<OP> client,
			ExceptionHandler handler) {
		this.client = client;
		this.handler = handler;
	}

	protected <T> void onError(ResultHolder<T> holder, RiakResponse response)
			throws Exception {
		try {
			this.handler.handle(AbstractRiakCommand.this, response);
		} finally {
			holder.fail(response.getMessage());
		}
	}

	public abstract class EaseHandler<T> implements RiakResponseHandler<T> {

		private final ResultHolder<?> holder;

		protected EaseHandler(ResultHolder<?> holder) {
			this.holder = holder;
		}

		@Override
		public void onError(RiakResponse response) throws Exception {
			AbstractRiakCommand.this.onError(this.holder, response);
		}
	}

	protected class SimpleEaseHandler<T> implements RiakResponseHandler<T> {

		final ResultHolder<T> holder;

		public SimpleEaseHandler(ResultHolder<T> holder) {
			this.holder = holder;
		}

		@Override
		public void onError(RiakResponse response) throws Exception {
			AbstractRiakCommand.this.onError(this.holder, response);
		}

		@Override
		public void handle(RiakContentsResponse<T> response) throws Exception {
			this.holder.setResult(response.getContents());
		}
	}
}
