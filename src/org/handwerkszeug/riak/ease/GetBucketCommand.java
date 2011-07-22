package org.handwerkszeug.riak.ease;

import org.handwerkszeug.riak.RiakAction;
import org.handwerkszeug.riak.RiakClient;
import org.handwerkszeug.riak.ease.internal.AbstractRiakCommand;
import org.handwerkszeug.riak.ease.internal.ResultHolder;
import org.handwerkszeug.riak.model.Bucket;
import org.handwerkszeug.riak.op.RiakOperations;

/**
 * @author taichi
 * @param <OP>
 */
public class GetBucketCommand<OP extends RiakOperations> extends
		AbstractRiakCommand<Bucket, OP> {

	final String bucket;

	public GetBucketCommand(RiakClient<OP> client, ExceptionHandler handler,
			String bucket) {
		super(client, handler);
		this.bucket = bucket;
	}

	@Override
	public Bucket execute() {
		final ResultHolder<Bucket> holder = new ResultHolder<Bucket>();
		this.client.execute(new RiakAction<OP>() {
			@Override
			public void execute(OP operations) {
				operations.getBucket(GetBucketCommand.this.bucket,
						new SimpleEaseHandler<Bucket>(holder));
			}
		});
		return holder.getResult();
	}
}
