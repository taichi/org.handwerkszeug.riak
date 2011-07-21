package org.handwerkszeug.riak.ease.internal;

import org.handwerkszeug.riak.RiakClient;
import org.handwerkszeug.riak.ease.ExceptionHandler;
import org.handwerkszeug.riak.ease.RiakCommand;
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

	protected abstract class EaseHandler<T> implements RiakResponseHandler<T> {

		final ResultHolder<T> holder;

		protected EaseHandler(ResultHolder<T> holder) {
			this.holder = holder;
		}

		@Override
		public void onError(RiakResponse response) throws Exception {
			try {
				AbstractRiakCommand.this.handler.handle(
						AbstractRiakCommand.this, response);
			} finally {
				this.holder.fail(response.getMessage());
			}
		}
	}
}
