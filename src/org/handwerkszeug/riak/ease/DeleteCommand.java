package org.handwerkszeug.riak.ease;

import static org.handwerkszeug.riak.util.Validation.notNull;

import org.handwerkszeug.riak.RiakAction;
import org.handwerkszeug.riak.RiakClient;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.ease.internal.AbstractRiakCommand;
import org.handwerkszeug.riak.ease.internal.ResultHolder;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.Quorum;
import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.op.RiakOperations;

/**
 * @author taichi
 * @param <OP>
 */
public class DeleteCommand<OP extends RiakOperations> extends
		AbstractRiakCommand<_, OP> {

	protected final Location location;

	protected Quorum readWrite;

	public DeleteCommand(RiakClient<OP> client, ExceptionHandler handler,
			Location location) {
		super(client, handler);
		this.location = location;
	}

	public DeleteCommand<OP> setReadWrite(Quorum quorum) {
		notNull(quorum, "quorum");
		this.readWrite = quorum;
		return this;
	}

	@Override
	public _ execute() {
		final ResultHolder<_> holder = new ResultHolder<_>();
		this.client.execute(new RiakAction<OP>() {
			@Override
			public void execute(OP operations) {
				EaseHandler<_> eh = new EaseHandler<_>(holder) {
					@Override
					public void handle(RiakContentsResponse<_> response)
							throws Exception {
						holder.setResult(_._);
					}
				};
				if (DeleteCommand.this.readWrite == null) {
					operations.delete(DeleteCommand.this.location, eh);
				} else {
					operations.delete(DeleteCommand.this.location,
							DeleteCommand.this.readWrite, eh);
				}
			}
		});
		return holder.getResult();
	}
}
