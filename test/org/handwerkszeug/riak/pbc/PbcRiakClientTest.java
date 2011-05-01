package org.handwerkszeug.riak.pbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import org.handwerkszeug.riak.Hosts;
import org.handwerkszeug.riak.RiakAction;
import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.model.RiakResponse;
import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PbcRiakClientTest {

	PbcRiakClient target;

	@Before
	public void setUp() throws Exception {
		target = new PbcRiakClient(Hosts.RIAK_PB_ADDR);
	}

	@After
	public void tearDown() throws Exception {
		target.dispose();
	}

	@Test
	public void testExecute() throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		target.execute(new RiakAction<PbcRiakOperations>() {
			@Override
			public void execute(PbcRiakOperations operations) {
				operations.ping(new RiakResponseHandler<String>() {
					@Override
					public void onError(RiakResponse response)
							throws RiakException {
						response.operationComplete();
						waiter.compareAndSet(false, true);
					}

					@Override
					public void handle(RiakContentsResponse<String> response)
							throws RiakException {
						try {
							assertEquals("pong", response.getContents());
							is[0] = true;
						} finally {
							response.operationComplete();
							waiter.compareAndSet(false, true);
						}
					}
				});
			}
		});

		wait(waiter, is);
	}

	protected void wait(final AtomicBoolean waiter, final boolean[] is)
			throws InterruptedException {
		while (waiter.get() == false) {
			Thread.sleep(10);
		}
		assertTrue(is[0]);
	}
}
