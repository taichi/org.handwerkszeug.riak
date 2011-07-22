package org.handwerkszeug.riak.ease;

import org.handwerkszeug.riak.RiakAction;
import org.handwerkszeug.riak.RiakClient;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.ease.internal.AbstractRiakCommand;
import org.handwerkszeug.riak.ease.internal.ResultHolder;
import org.handwerkszeug.riak.model.Bucket;
import org.handwerkszeug.riak.op.RiakOperations;

/**
 * @author taichi
 * @param <OP>
 */
public class SetBucketCommand<OP extends RiakOperations> extends
		AbstractRiakCommand<_, OP> {

	final Bucket bucket;

	public SetBucketCommand(RiakClient<OP> client, ExceptionHandler handler,
			Bucket bucket) {
		super(client, handler);
		this.bucket = bucket;
	}

	@Override
	public _ execute() {
		final ResultHolder<_> holder = new ResultHolder<_>();
		this.client.execute(new RiakAction<OP>() {
			@Override
			public void execute(OP operations) {
				operations.setBucket(SetBucketCommand.this.bucket,
						new SimpleEaseHandler<_>(holder));
			}
		});
		return holder.getResult();
	}
}
