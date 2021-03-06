package org.handwerkszeug.riak.ease;

import org.handwerkszeug.riak.RiakAction;
import org.handwerkszeug.riak.RiakClient;
import org.handwerkszeug.riak.ease.internal.AbstractRiakCommand;
import org.handwerkszeug.riak.ease.internal.ExecutionDelegate;
import org.handwerkszeug.riak.ease.internal.ResultHolder;
import org.handwerkszeug.riak.model.PostOptions;
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

	protected Quorum read;
	protected Quorum write;
	protected Quorum durableWrite;

	protected boolean returnBody;

	public PostCommand(RiakClient<OP> client, ExceptionHandler handler,
			RiakObject<byte[]> content) {
		super(client, handler);
		this.content = content;
	}

	public PostCommand<OP> read(Quorum quorum) {
		this.read = quorum;
		this.delegate = optionalExecution;
		return this;
	}

	public PostCommand<OP> write(Quorum quorum) {
		this.write = quorum;
		this.delegate = optionalExecution;
		return this;
	}

	public PostCommand<OP> durableWrite(Quorum quorum) {
		this.durableWrite = quorum;
		this.delegate = optionalExecution;
		return this;
	}

	public PostCommand<OP> returnBody(boolean is) {
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

	static final ExecutionDelegate<RiakObject<byte[]>, PostCommand<?>> defaultExecution = new ExecutionDelegate<RiakObject<byte[]>, PostCommand<?>>() {
		@Override
		public <RO extends RiakOperations> void execute(PostCommand<?> cmd,
				RO operations, ResultHolder<RiakObject<byte[]>> holder) {
			operations.post(cmd.content,
					cmd.new SimpleEaseHandler<RiakObject<byte[]>>(holder));
		};
	};
	static final ExecutionDelegate<RiakObject<byte[]>, PostCommand<?>> optionalExecution = new ExecutionDelegate<RiakObject<byte[]>, PostCommand<?>>() {
		@Override
		public <RO extends RiakOperations> void execute(
				final PostCommand<?> cmd, RO operations,
				ResultHolder<RiakObject<byte[]>> holder) {
			PostOptions options = new PostOptions() {
				@Override
				public Quorum getWriteQuorum() {
					return cmd.write;
				}

				@Override
				public Quorum getDurableWriteQuorum() {
					return cmd.durableWrite;
				}

				@Override
				public boolean getReturnBody() {
					return cmd.returnBody;
				}
			};
			operations.post(cmd.content, options,
					cmd.new SimpleEaseHandler<RiakObject<byte[]>>(holder));
		};
	};
}
