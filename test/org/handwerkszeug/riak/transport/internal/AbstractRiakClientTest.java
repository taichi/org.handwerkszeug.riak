package org.handwerkszeug.riak.transport.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;

import org.handwerkszeug.riak.RiakAction;
import org.handwerkszeug.riak.RiakClient;
import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.model.RiakFuture;
import org.handwerkszeug.riak.model.RiakResponse;
import org.handwerkszeug.riak.op.RiakOperations;
import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author taichi
 */
public abstract class AbstractRiakClientTest<OP extends RiakOperations> {

	RiakClient<OP> target;

	@Before
	public void setUp() throws Exception {
		this.target = newTarget();
	}

	protected abstract RiakClient<OP> newTarget();

	@After
	public void tearDown() throws Exception {
		this.target.dispose();
	}

	@Test
	public void testExecute() throws Exception {
		this.target.execute(new RiakAction<OP>() {
			@Override
			public void execute(OP operations) {
				final String[] actual = new String[1];
				RiakFuture waiter = operations
						.ping(new RiakResponseHandler<String>() {
							@Override
							public void onError(RiakResponse response)
									throws Exception {
								fail(response.getMessage());
							}

							@Override
							public void handle(
									RiakContentsResponse<String> response)
									throws Exception {
								actual[0] = response.getContents();
								response.operationComplete();
							}
						});
				try {
					assertTrue("test is timeout.",
							waiter.await(3, TimeUnit.SECONDS));
					assertEquals("pong", actual[0]);
				} catch (InterruptedException e) {
					fail(e.getMessage());
				}
			}
		});
	}

}
