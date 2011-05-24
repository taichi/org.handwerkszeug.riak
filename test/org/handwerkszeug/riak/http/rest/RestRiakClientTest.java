package org.handwerkszeug.riak.http.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.handwerkszeug.riak.Hosts;
import org.handwerkszeug.riak.RiakAction;
import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak.model.RiakContentsResponse;
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
		this.target = new RestRiakClient(RestConfig.newConfig(Hosts.RIAK_HOST));
	}

	@Test
	public void testExecute() throws Exception {
		final CountDownLatch waiter = new CountDownLatch(1);
		final boolean[] is = { false };

		this.target.execute(new RiakAction<RestRiakOperations>() {
			@Override
			public void execute(RestRiakOperations operations) {
				operations.ping(new RiakResponseHandler<String>() {
					@Override
					public void onError(RiakResponse response)
							throws RiakException {
						response.operationComplete();
						waiter.countDown();
					}

					@Override
					public void handle(RiakContentsResponse<String> response)
							throws RiakException {
						try {
							assertEquals("pong", response.getContents());
							is[0] = true;
						} finally {
							response.operationComplete();
							waiter.countDown();
						}
					}
				});
			}
		});

		wait(waiter, is);
	}

	protected void wait(final CountDownLatch waiter, final boolean[] is)
			throws InterruptedException {
		assertTrue("test is timeout.", waiter.await(3, TimeUnit.SECONDS));
		assertTrue(is[0]);
	}

	@After
	public void tearDown() {
		this.target.dispose();
	}
}
