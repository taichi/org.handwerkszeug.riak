package org.handwerkszeug.riak.ease;

import static org.handwerkszeug.riak.util.Validation.notNull;

import java.util.Date;

import org.handwerkszeug.riak.RiakAction;
import org.handwerkszeug.riak.RiakClient;
import org.handwerkszeug.riak.ease.internal.AbstractRiakCommand;
import org.handwerkszeug.riak.ease.internal.ResultHolder;
import org.handwerkszeug.riak.model.GetOptions;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.Quorum;
import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.op.RiakOperations;
import org.handwerkszeug.riak.op.RiakResponseHandler;

/**
 * @author taichi
 * @param <OP>
 */
public class GetCommand<OP extends RiakOperations> extends
		AbstractRiakCommand<RiakObject<byte[]>, OP> {

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
		return this;
	}

	public GetCommand<OP> setIfNoneMatch(String etag) {
		notNull(etag, "etag");
		this.ifNoneMatch = etag;
		return this;
	}

	public GetCommand<OP> setIfMatch(String etag) {
		notNull(etag, "etag");
		this.ifMatch = etag;
		return this;
	}

	public GetCommand<OP> setIfModifiedSince(Date date) {
		notNull(date, "date");
		this.ifModifiedSince = date;
		return this;
	}

	GetOptions toOptions() {
		if (this.readQuorum == null && this.ifNoneMatch == null
				&& this.ifMatch == null && this.ifModifiedSince == null) {
			return null;
		}
		return new GetOptions() {
			@Override
			public Quorum getReadQuorum() {
				return GetCommand.this.readQuorum;
			}

			@Override
			public String getIfNoneMatch() {
				return GetCommand.this.ifNoneMatch;
			}

			@Override
			public Date getIfModifiedSince() {
				return GetCommand.this.ifModifiedSince;
			}

			@Override
			public String getIfMatch() {
				return GetCommand.this.ifMatch;
			}
		};
	}

	@Override
	public RiakObject<byte[]> execute() {
		final ResultHolder<RiakObject<byte[]>> holder = new ResultHolder<RiakObject<byte[]>>();
		this.client.execute(new RiakAction<OP>() {
			@Override
			public void execute(OP operations) {
				RiakResponseHandler<RiakObject<byte[]>> rrh = new EaseHandler<RiakObject<byte[]>>(
						holder) {
					@Override
					public void handle(
							RiakContentsResponse<RiakObject<byte[]>> response)
							throws Exception {
						holder.setResult(response.getContents());
					}
				};
				GetOptions opt = toOptions();
				if (opt == null) {
					operations.get(GetCommand.this.location, rrh);
				} else {
					operations.get(GetCommand.this.location, opt, rrh);
				}
			}
		});
		return holder.getResult();
	}
}
