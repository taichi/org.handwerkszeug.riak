package org.handwerkszeug.riak.ease;

import org.handwerkszeug.riak.RiakAction;
import org.handwerkszeug.riak.RiakClient;
import org.handwerkszeug.riak.ease.internal.AbstractRiakCommand;
import org.handwerkszeug.riak.ease.internal.ResultHolder;
import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.op.RiakOperations;

/**
 * @author taichi
 * @param <OP>
 */
public class PingCommand<OP extends RiakOperations> extends
		AbstractRiakCommand<String, OP> {

	public PingCommand(RiakClient<OP> client, ExceptionHandler handler) {
		super(client, handler);
	}

	@Override
	public String execute() {
		final ResultHolder<String> holder = new ResultHolder<String>();
		this.client.execute(new RiakAction<OP>() {
			@Override
			public void execute(OP operations) {
				operations.ping(new EaseHandler<String>(holder) {
					@Override
					public void handle(RiakContentsResponse<String> response)
							throws Exception {
						holder.setResult(response.getContents());
					}
				});
			}
		});
		return holder.getResult();
	}
}
