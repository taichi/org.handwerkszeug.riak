package org.handwerkszeug.riak.ease;

import org.handwerkszeug.riak.RiakAction;
import org.handwerkszeug.riak.RiakClient;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.ease.internal.AbstractRiakCommand;
import org.handwerkszeug.riak.ease.internal.ResultHolder;
import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.op.RiakOperations;

/**
 * @author taichi
 * @param <OP>
 */
public class PutCommand<OP extends RiakOperations> extends
		AbstractRiakCommand<_, OP> {

	protected final RiakObject<byte[]> content;

	public PutCommand(RiakClient<OP> client, ExceptionHandler handler,
			RiakObject<byte[]> content) {
		super(client, handler);
		this.content = content;
	}

	@Override
	public _ execute() {
		final ResultHolder<_> holder = new ResultHolder<_>();
		this.client.execute(new RiakAction<OP>() {
			@Override
			public void execute(OP operations) {
				operations.put(PutCommand.this.content, new EaseHandler<_>(
						holder) {
					@Override
					public void handle(RiakContentsResponse<_> response)
							throws Exception {
						holder.setResult(_._);
					}
				});
			}
		});
		return holder.getResult();
	}
}
