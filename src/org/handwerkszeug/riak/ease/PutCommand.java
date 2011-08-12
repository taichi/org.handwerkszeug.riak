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

	protected Quorum write;
	protected Quorum durableWrite;

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

	public PutCommand<OP> write(Quorum quorum) {
		this.write = quorum;
		this.delegate = optionalExecution;
		return this;
	}

	public PutCommand<OP> durableWrite(Quorum quorum) {
		this.durableWrite = quorum;
		this.delegate = optionalExecution;
		return this;
	}

	public PutCommand<OP> returnBody(boolean is) {
		this.returnBody = is;
		this.delegate = optionalExecution;
		return this;
	}

	public PutCommand<OP> ifNoneMatch(String etag) {
		this.ifNoneMatch = etag;
		this.delegate = optionalExecution;
		return this;
	}

	public PutCommand<OP> ifMatch(String etag) {
		this.ifMatch = etag;
		this.delegate = optionalExecution;
		return this;
	}

	public PutCommand<OP> ifModifiedSince(Date since) {
		this.ifModifiedSince = since;
		this.delegate = optionalExecution;
		return this;
	}

	public PutCommand<OP> ifUnmodifiedSince(Date since) {
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
