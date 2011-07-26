package org.handwerkszeug.riak.ease;

import java.util.Date;

import org.handwerkszeug.riak.RiakAction;
import org.handwerkszeug.riak.RiakClient;
import org.handwerkszeug.riak.ease.internal.AbstractRiakCommand;
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
		return this;
	}

	public PutCommand<OP> setReadQuorum(Quorum quorum) {
		this.readQuorum = quorum;
		return this;
	}

	public PutCommand<OP> setWriteQuorum(Quorum quorum) {
		this.writeQuorum = quorum;
		return this;
	}

	public PutCommand<OP> setDurableWriteQuorum(Quorum quorum) {
		this.durableWriteQuorum = quorum;
		return this;
	}

	public PutCommand<OP> setReturnBody(boolean is) {
		this.returnBody = is;
		return this;
	}

	public PutCommand<OP> setIfNoneMatch(String etag) {
		this.ifNoneMatch = etag;
		return this;
	}

	public PutCommand<OP> setIfMatch(String etag) {
		this.ifMatch = etag;
		return this;
	}

	public PutCommand<OP> setIfModifiedSince(Date since) {
		this.ifModifiedSince = since;
		return this;
	}

	public PutCommand<OP> setIfUnmodifiedSince(Date since) {
		this.ifUnmodifiedSince = since;
		return this;
	}

	@Override
	public RiakObject<byte[]> execute() {
		final ResultHolder<RiakObject<byte[]>> holder = new ResultHolder<RiakObject<byte[]>>();
		this.client.execute(new RiakAction<OP>() {
			@Override
			public void execute(OP operations) {
				PutOptions options = new PutOptions() {

					@Override
					public String getVectorClock() {
						return PutCommand.this.vectorClock;
					}

					@Override
					public Quorum getReadQuorum() {
						return PutCommand.this.readQuorum;
					}

					@Override
					public Quorum getWriteQuorum() {
						return PutCommand.this.writeQuorum;
					}

					@Override
					public Quorum getDurableWriteQuorum() {
						return PutCommand.this.durableWriteQuorum;
					}

					@Override
					public boolean getReturnBody() {
						return PutCommand.this.returnBody;
					}

					@Override
					public String getIfNoneMatch() {
						return PutCommand.this.ifNoneMatch;
					}

					@Override
					public String getIfMatch() {
						return PutCommand.this.ifMatch;
					}

					@Override
					public Date getIfModifiedSince() {
						return PutCommand.this.ifModifiedSince;
					}

					@Override
					public Date getIfUnmodifiedSince() {
						return PutCommand.this.ifUnmodifiedSince;
					}

				};

				operations.put(PutCommand.this.content, options,
						new EaseSiblingHandler(holder));
			}
		});
		return holder.getResult();
	}

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
		public void end(RiakResponse response) throws Exception {
			this.holder.setResult(this.content);
		}
	}
}
