package org.handwerkszeug.riak.ease;

import org.handwerkszeug.riak.RiakAction;
import org.handwerkszeug.riak.RiakClient;
import org.handwerkszeug.riak.ease.internal.AbstractRiakCommand;
import org.handwerkszeug.riak.ease.internal.ExecutionDelegate;
import org.handwerkszeug.riak.ease.internal.ResultHolder;
import org.handwerkszeug.riak.model.Quorum;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.op.RiakOperations;

/**
 * @author taichi
 * @param <OP>
 */
public class PostCommand<OP extends RiakOperations> extends
		AbstractRiakCommand<RiakObject<byte[]>, OP> {

	protected ExecutionDelegate<RiakObject<byte[]>, PostCommand<?>> delegate = defaultExecution;

	protected final RiakObject<byte[]> content;

	protected Quorum readQuorum;
	protected Quorum writeQuorum;
	protected Quorum durableWriteQuorum;

	protected boolean returnBody;

	public PostCommand(RiakClient<OP> client, ExceptionHandler handler,
			RiakObject<byte[]> content) {
		super(client, handler);
		this.content = content;
	}

	public PostCommand<OP> setReadQuorum(Quorum quorum) {
		this.readQuorum = quorum;
		this.delegate = optionalExecution;
		return this;
	}

	public PostCommand<OP> setWriteQuorum(Quorum quorum) {
		this.writeQuorum = quorum;
		this.delegate = optionalExecution;
		return this;
	}

	public PostCommand<OP> setDurableWriteQuorum(Quorum quorum) {
		this.durableWriteQuorum = quorum;
		this.delegate = optionalExecution;
		return this;
	}

	public PostCommand<OP> setReturnBody(boolean is) {
		this.returnBody = is;
		this.delegate = optionalExecution;
		return this;
	}

	@Override
	public RiakObject<byte[]> execute() {
		final ResultHolder<RiakObject<byte[]>> holder = new ResultHolder<RiakObject<byte[]>>();
		this.client.execute(new RiakAction<OP>() {
			@Override
			public void execute(OP operations) {
				PostCommand.this.delegate.execute(PostCommand.this, operations,
						holder);
			}
		});
		return holder.getResult();
	}

	static final ExecutionDelegate<RiakObject<byte[]>, PostCommand<?>> defaultExecution = null;
	static final ExecutionDelegate<RiakObject<byte[]>, PostCommand<?>> optionalExecution = null;
}
