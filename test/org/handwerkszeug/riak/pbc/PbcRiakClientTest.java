package org.handwerkszeug.riak.pbc;

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
public class PbcRiakClientTest {

	PbcRiakClient target;

	@Before
	public void setUp() throws Exception {
		PbcConfig config = PbcConfig.newConfig(Hosts.RIAK_HOST);
		this.target = new PbcRiakClient(config);
	}

	@After
	public void tearDown() throws Exception {
		this.target.dispose();
	}

	@Test
	public void testExecute() throws Exception {
		final CountDownLatch waiter = new CountDownLatch(1);
		final boolean[] is = { false };

		this.target.execute(new RiakAction<PbcRiakOperations>() {
			@Override
			public void execute(PbcRiakOperations operations) {
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
}
