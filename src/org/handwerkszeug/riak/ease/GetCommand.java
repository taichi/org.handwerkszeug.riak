package org.handwerkszeug.riak.ease;

import static org.handwerkszeug.riak.util.Validation.notNull;

import java.util.Date;

import org.handwerkszeug.riak.RiakAction;
import org.handwerkszeug.riak.RiakClient;
import org.handwerkszeug.riak.ease.internal.AbstractRiakCommand;
import org.handwerkszeug.riak.ease.internal.ExecutionDelegate;
import org.handwerkszeug.riak.ease.internal.ResultHolder;
import org.handwerkszeug.riak.model.GetOptions;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.Quorum;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.op.RiakOperations;

/**
 * @author taichi
 * @param <OP>
 */
public class GetCommand<OP extends RiakOperations> extends
		AbstractRiakCommand<RiakObject<byte[]>, OP> {

	protected ExecutionDelegate<RiakObject<byte[]>, GetCommand<?>> delegate = defaultExecution;

	protected final Location location;

	protected Quorum readQuorum;
	protected String ifNoneMatch;
	protected String ifMatch;
	protected Date ifModifiedSince;

	public GetCommand(RiakClient<OP> client, ExceptionHandler handler,
			Location location) {
		super(client, handler);
		this.location = location;
	}

	/**
	 * how many replicas need to agree when retrieving the object; possible
	 * values include ‘default’, ‘one’, ‘quorum’, ‘all’, or any integer <= N
	 * (default is defined per the bucket)
	 */
	public GetCommand<OP> setReadQuorum(Quorum quorum) {
		notNull(quorum, "quorum");
		this.readQuorum = quorum;
		this.delegate = optionalExecution;
		return this;
	}

	public GetCommand<OP> setIfNoneMatch(String etag) {
		notNull(etag, "etag");
		this.ifNoneMatch = etag;
		this.delegate = optionalExecution;
		return this;
	}

	public GetCommand<OP> setIfMatch(String etag) {
		notNull(etag, "etag");
		this.ifMatch = etag;
		this.delegate = optionalExecution;
		return this;
	}

	public GetCommand<OP> setIfModifiedSince(Date since) {
		notNull(since, "since");
		this.ifModifiedSince = since;
		this.delegate = optionalExecution;
		return this;
	}

	@Override
	public RiakObject<byte[]> execute() {
		final ResultHolder<RiakObject<byte[]>> holder = new ResultHolder<RiakObject<byte[]>>();
		this.client.execute(new RiakAction<OP>() {
			@Override
			public void execute(OP operations) {
				GetCommand.this.delegate.execute(GetCommand.this, operations,
						holder);
			}
		});
		return holder.getResult();
	}

	static final ExecutionDelegate<RiakObject<byte[]>, GetCommand<?>> defaultExecution = new ExecutionDelegate<RiakObject<byte[]>, GetCommand<?>>() {
		@Override
		public <RO extends RiakOperations> void execute(GetCommand<?> cmd,
				RO operations, ResultHolder<RiakObject<byte[]>> holder) {
			operations.get(cmd.location,
					cmd.new SimpleEaseHandler<RiakObject<byte[]>>(holder));
		}
	};

	static final ExecutionDelegate<RiakObject<byte[]>, GetCommand<?>> optionalExecution = new ExecutionDelegate<RiakObject<byte[]>, GetCommand<?>>() {
		@Override
		public <RO extends RiakOperations> void execute(
				final GetCommand<?> cmd, RO operations,
				ResultHolder<RiakObject<byte[]>> holder) {
			GetOptions opt = new GetOptions() {
				@Override
				public Quorum getReadQuorum() {
					return cmd.readQuorum;
				}

				@Override
				public String getIfNoneMatch() {
					return cmd.ifNoneMatch;
				}

				@Override
				public Date getIfModifiedSince() {
					return cmd.ifModifiedSince;
				}

				@Override
				public String getIfMatch() {
					return cmd.ifMatch;
				}
			};
			operations.get(cmd.location, opt,
					cmd.new SimpleEaseHandler<RiakObject<byte[]>>(holder));
		}
	};
}
