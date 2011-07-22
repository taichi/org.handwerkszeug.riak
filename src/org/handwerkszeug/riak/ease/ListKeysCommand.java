package org.handwerkszeug.riak.ease;

import java.util.ArrayList;
import java.util.List;

import org.handwerkszeug.riak.RiakAction;
import org.handwerkszeug.riak.RiakClient;
import org.handwerkszeug.riak.ease.internal.AbstractRiakCommand;
import org.handwerkszeug.riak.ease.internal.ResultHolder;
import org.handwerkszeug.riak.model.KeyResponse;
import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.op.RiakOperations;

/**
 * @author taichi
 * @param <OP>
 */
public class ListKeysCommand<OP extends RiakOperations> extends
		AbstractRiakCommand<List<String>, OP> {

	final String bucket;

	public ListKeysCommand(RiakClient<OP> client, ExceptionHandler handler,
			String bucket) {
		super(client, handler);
		this.bucket = bucket;
	}

	@Override
	public List<String> execute() {
		final ResultHolder<List<String>> holder = new ResultHolder<List<String>>();
		final List<String> keys = new ArrayList<String>();
		this.client.execute(new RiakAction<OP>() {
			@Override
			public void execute(OP operations) {
				operations.listKeys(ListKeysCommand.this.bucket,
						new EaseHandler<KeyResponse>(holder) {
							@Override
							public void handle(
									RiakContentsResponse<KeyResponse> response)
									throws Exception {
								KeyResponse kr = response.getContents();
								if (kr.getDone()) {
									holder.setResult(keys);
								} else {
									keys.addAll(kr.getKeys());
								}
							}
						});
			}
		});
		return holder.getResult();
	}
}
