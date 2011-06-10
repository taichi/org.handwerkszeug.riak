package org.handwerkszeug.riak.transport.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;

import org.handwerkszeug.riak.Hosts;
import org.handwerkszeug.riak.RiakAction;
import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.model.RiakFuture;
import org.handwerkszeug.riak.model.RiakResponse;
import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author taichi
 */
public class RestRiakClientTest {

	RestRiakClient target;

	@Before
	public void setUp() throws Exception {
		this.target = new RestRiakClient(
				RestRiakConfig.newConfig(Hosts.RIAK_HOST));
	}

	@After
	public void tearDown() {
		this.target.dispose();
	}

	@Test
	public void testExecute() throws Exception {
		this.target.execute(new RiakAction<RestRiakOperations>() {
			@Override
			public void execute(RestRiakOperations operations) {
				final boolean[] is = { false };
				RiakFuture waiter = operations
						.ping(new RiakResponseHandler<String>() {
							@Override
							public void onError(RiakResponse response)
									throws RiakException {
								response.operationComplete();
							}

							@Override
							public void handle(
									RiakContentsResponse<String> response)
									throws RiakException {
								try {
									assertEquals("pong", response.getContents());
									is[0] = true;
								} finally {
									response.operationComplete();
								}
							}
						});
				try {
					assertTrue("test is timeout.",
							waiter.await(3, TimeUnit.SECONDS));
					assertTrue(is[0]);
				} catch (InterruptedException e) {
					fail(e.getMessage());
				}

			}
		});
	}

}
