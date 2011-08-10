package org.handwerkszeug.riak.transport.rest;

import static org.handwerkszeug.riak.util.Validation.notNull;

import java.util.ArrayList;
import java.util.List;

import org.handwerkszeug.riak.RiakAction;
import org.handwerkszeug.riak.RiakClient;
import org.handwerkszeug.riak.ease.ExceptionHandler;
import org.handwerkszeug.riak.ease.internal.AbstractRiakCommand;
import org.handwerkszeug.riak.ease.internal.ResultHolder;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.RiakObject;

/**
 * @author taichi
 * @param <OP>
 */
public class LinkWalkingCommand extends
		AbstractRiakCommand<List<RiakObject<byte[]>>, RestRiakOperations> {

	final Location begin;

	List<LinkCondition> steps = new ArrayList<LinkCondition>();

	public LinkWalkingCommand(RiakClient<RestRiakOperations> client,
			ExceptionHandler handler, Location begin) {
		super(client, handler);
		this.begin = begin;
	}

	@Override
	public List<RiakObject<byte[]>> execute() {
		final ResultHolder<List<RiakObject<byte[]>>> holder = new ResultHolder<List<RiakObject<byte[]>>>();
		this.client.execute(new RiakAction<RestRiakOperations>() {
			@Override
			public void execute(RestRiakOperations operations) {
				operations
						.walk(LinkWalkingCommand.this.begin,
								LinkWalkingCommand.this.steps,
								new SimpleEaseHandler<List<RiakObject<byte[]>>>(
										holder));
			}
		});

		return holder.getResult();
	}

	public LinkWalkingCommand add(String bucket, String tag) {
		return this.add(bucket, tag, false);
	}

	public LinkWalkingCommand add(String bucket, String tag, boolean keep) {
		return this.add(new LinkCondition(bucket, tag, keep));
	}

	public LinkWalkingCommand add(LinkCondition condition) {
		notNull(condition, "condition");
		this.steps.add(condition);
		return this;
	}
}
