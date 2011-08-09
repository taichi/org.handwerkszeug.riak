package org.handwerkszeug.riak.ease.internal;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.node.ArrayNode;
import org.handwerkszeug.riak.Markers;
import org.handwerkszeug.riak.RiakAction;
import org.handwerkszeug.riak.RiakClient;
import org.handwerkszeug.riak.ease.ExceptionHandler;
import org.handwerkszeug.riak.mapreduce.MapReduceResponse;
import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.op.RiakOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author taichi
 * @param <OP>
 */
public class MapReduceCommand<OP extends RiakOperations> extends
		AbstractRiakCommand<List<ArrayNode>, OP> {

	static final Logger LOG = LoggerFactory.getLogger(MapReduceCommand.class);

	final String query;

	public MapReduceCommand(RiakClient<OP> client, ExceptionHandler handler,
			String query) {
		super(client, handler);
		if (LOG.isDebugEnabled()) {
			LOG.debug(Markers.BOUNDARY, query);
		}
		this.query = query;
	}

	@Override
	public List<ArrayNode> execute() {
		final ResultHolder<List<ArrayNode>> holder = new ResultHolder<List<ArrayNode>>();
		final List<ArrayNode> list = new ArrayList<ArrayNode>();
		this.client.execute(new RiakAction<OP>() {
			@Override
			public void execute(OP operations) {
				operations.mapReduce(MapReduceCommand.this.query,
						new EaseHandler<MapReduceResponse>(holder) {
							@Override
							public void handle(
									RiakContentsResponse<MapReduceResponse> response)
									throws Exception {
								MapReduceResponse mrr = response.getContents();
								if (mrr.getDone()) {
									holder.setResult(list);
								} else {
									list.add(mrr.getResponse());
								}
							}
						});
			}
		});
		return holder.getResult();
	}
}
