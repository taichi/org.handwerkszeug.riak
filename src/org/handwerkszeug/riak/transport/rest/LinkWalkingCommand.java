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
import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.model.RiakObject;

/**
 * @author taichi
 * @param <OP>
 */
public class LinkWalkingCommand extends
		AbstractRiakCommand<List<List<RiakObject<byte[]>>>, RestRiakOperations> {

	final Location begin;

	List<LinkCondition> steps = new ArrayList<LinkCondition>();

	public LinkWalkingCommand(RiakClient<RestRiakOperations> client,
			ExceptionHandler handler, Location begin) {
		super(client, handler);
		this.begin = begin;
	}

	@Override
	public List<List<RiakObject<byte[]>>> execute() {
		final List<List<RiakObject<byte[]>>> list = new ArrayList<List<RiakObject<byte[]>>>();
		final ResultHolder<List<List<RiakObject<byte[]>>>> holder = new ResultHolder<List<List<RiakObject<byte[]>>>>();
		this.client.execute(new RiakAction<RestRiakOperations>() {
			@Override
			public void execute(RestRiakOperations operations) {
				operations.walk(LinkWalkingCommand.this.begin,
						LinkWalkingCommand.this.steps,
						new EaseHandler<LinkWalkingResponse>(holder) {
							@Override
							public void handle(
									RiakContentsResponse<LinkWalkingResponse> response)
									throws Exception {
								LinkWalkingResponse contents = response
										.getContents();
								if (contents.getDone()) {
									holder.setResult(list);
								} else {
									list.add(contents.getResponse());
								}
							}
						});
			}
		});

		return holder.getResult();
	}

	public LinkWalkingCommand step(String bucket, String tag) {
		return this.step(bucket, tag, false);
	}

	public LinkWalkingCommand step(String bucket, String tag, boolean keep) {
		return this.step(new LinkCondition(bucket, tag, keep));
	}

	public LinkWalkingCommand step(LinkCondition condition) {
		notNull(condition, "condition");
		this.steps.add(condition);
		return this;
	}
}
