package org.handwerkszeug.riak.ease;

import java.util.Date;

import org.handwerkszeug.riak.RiakAction;
import org.handwerkszeug.riak.RiakClient;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.ease.internal.AbstractRiakCommand;
import org.handwerkszeug.riak.ease.internal.ExecutionDelegate;
import org.handwerkszeug.riak.ease.internal.ResultHolder;
import org.handwerkszeug.riak.model.PutOptions;
import org.handwerkszeug.riak.model.Quorum;
import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.model.RiakResponse;
import org.handwerkszeug.riak.op.RiakOperations;
import org.handwerkszeug.riak.op.SiblingHandler;

/**
 * @author taichi
 * @param <OP>
 */
public class PutCommand<OP extends RiakOperations> extends
		AbstractRiakCommand<RiakObject<byte[]>, OP> {

	protected ExecutionDelegate<RiakObject<byte[]>, PutCommand<?>> delegate = defaultExecution;

	protected final RiakObject<byte[]> content;

	protected String vectorClock;

	protected Quorum readQuorum;
	protected Quorum writeQuorum;
	protected Quorum durableWriteQuorum;

	protected boolean returnBody;

	protected String ifNoneMatch;
	protected String ifMatch;

	protected Date ifModifiedSince;
	protected Date ifUnmodifiedSince;

	public PutCommand(RiakClient<OP> client, ExceptionHandler handler,
			RiakObject<byte[]> content) {
		super(client, handler);
		this.content = content;
	}

	public PutCommand<OP> setVectorClock(String vectorClock) {
		this.vectorClock = vectorClock;
		this.delegate = optionalExecution;
		return this;
	}

	public PutCommand<OP> setReadQuorum(Quorum quorum) {
		this.readQuorum = quorum;
		this.delegate = optionalExecution;
		return this;
	}

	public PutCommand<OP> setWriteQuorum(Quorum quorum) {
		this.writeQuorum = quorum;
		this.delegate = optionalExecution;
		return this;
	}

	public PutCommand<OP> setDurableWriteQuorum(Quorum quorum) {
		this.durableWriteQuorum = quorum;
		this.delegate = optionalExecution;
		return this;
	}

	public PutCommand<OP> setReturnBody(boolean is) {
		this.returnBody = is;
		this.delegate = optionalExecution;
		return this;
	}

	public PutCommand<OP> setIfNoneMatch(String etag) {
		this.ifNoneMatch = etag;
		this.delegate = optionalExecution;
		return this;
	}

	public PutCommand<OP> setIfMatch(String etag) {
		this.ifMatch = etag;
		this.delegate = optionalExecution;
		return this;
	}

	public PutCommand<OP> setIfModifiedSince(Date since) {
		this.ifModifiedSince = since;
		this.delegate = optionalExecution;
		return this;
	}

	public PutCommand<OP> setIfUnmodifiedSince(Date since) {
		this.ifUnmodifiedSince = since;
		this.delegate = optionalExecution;
		return this;
	}

	@Override
	public RiakObject<byte[]> execute() {
		final ResultHolder<RiakObject<byte[]>> holder = new ResultHolder<RiakObject<byte[]>>();
		this.client.execute(new RiakAction<OP>() {
			@Override
			public void execute(OP operations) {
				PutCommand.this.delegate.execute(PutCommand.this, operations,
						holder);
			}
		});
		return holder.getResult();
	}

	static final ExecutionDelegate<RiakObject<byte[]>, PutCommand<?>> defaultExecution = new ExecutionDelegate<RiakObject<byte[]>, PutCommand<?>>() {
		@Override
		public <RO extends RiakOperations> void execute(PutCommand<?> cmd,
				RO operations, final ResultHolder<RiakObject<byte[]>> holder) {
			operations.put(cmd.content, cmd.new EaseHandler<_>(holder) {
				@Override
				public void handle(RiakContentsResponse<_> response)
						throws Exception {
					holder.setResult(null);
				}
			});
		}
	};

	static final ExecutionDelegate<RiakObject<byte[]>, PutCommand<?>> optionalExecution = new ExecutionDelegate<RiakObject<byte[]>, PutCommand<?>>() {
		@Override
		public <RO extends RiakOperations> void execute(
				final PutCommand<?> cmd, RO operations,
				final ResultHolder<RiakObject<byte[]>> holder) {
			PutOptions options = new PutOptions() {

				@Override
				public String getVectorClock() {
					return cmd.vectorClock;
				}

				@Override
				public Quorum getReadQuorum() {
					return cmd.readQuorum;
				}

				@Override
				public Quorum getWriteQuorum() {
					return cmd.writeQuorum;
				}

				@Override
				public Quorum getDurableWriteQuorum() {
					return cmd.durableWriteQuorum;
				}

				@Override
				public boolean getReturnBody() {
					return cmd.returnBody;
				}

				@Override
				public String getIfNoneMatch() {
					return cmd.ifNoneMatch;
				}

				@Override
				public String getIfMatch() {
					return cmd.ifMatch;
				}

				@Override
				public Date getIfModifiedSince() {
					return cmd.ifModifiedSince;
				}

				@Override
				public Date getIfUnmodifiedSince() {
					return cmd.ifUnmodifiedSince;
				}

			};
			operations.put(cmd.content, options, cmd.new EaseSiblingHandler(
					holder));
		}
	};

	class EaseSiblingHandler implements SiblingHandler {

		protected final ResultHolder<RiakObject<byte[]>> holder;

		protected RiakObject<byte[]> content;

		public EaseSiblingHandler(ResultHolder<RiakObject<byte[]>> holder) {
			this.holder = holder;
		}

		@Override
		public void onError(RiakResponse response) throws Exception {
			PutCommand.this.onError(this.holder, response);
		}

		@Override
		public void begin() throws Exception {
		}

		@Override
		public void handle(RiakContentsResponse<RiakObject<byte[]>> response)
				throws Exception {
			this.content = response.getContents();
		}

		@Override
		public void end() throws Exception {
			this.holder.setResult(this.content);
		}
	}
}
