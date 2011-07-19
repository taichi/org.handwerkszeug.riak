package org.handwerkszeug.riak.transport.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.handwerkszeug.riak.RiakAction;
import org.handwerkszeug.riak.RiakClient;
import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.model.RiakFuture;
import org.handwerkszeug.riak.op.RiakOperations;
import org.handwerkszeug.riak.op.TestingHandler;
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
						.ping(new TestingHandler<String>() {
							@Override
							public void handle(
									RiakContentsResponse<String> response)
									throws Exception {
								actual[0] = response.getContents();
							}
						});
				try {
					assertTrue("test is timeout.",
							waiter.await(3, TimeUnit.SECONDS));
					assertEquals("pong", actual[0]);
					fail();
				} catch (InterruptedException e) {
					fail(e.getMessage());
				}
			}
		});
	}

	@Test
	public void testExecute2() throws Exception {
		final String[] actual = new String[1];
		final CountDownLatch latch = new CountDownLatch(1);
		this.target.execute(new RiakAction<OP>() {
			@Override
			public void execute(OP operations) {
				operations.ping(new TestingHandler<String>() {
					@Override
					public void handle(RiakContentsResponse<String> response)
							throws Exception {
						actual[0] = response.getContents();
						latch.countDown();
					}
				});
			}
		});
		assertTrue("test is timeout.", latch.await(3, TimeUnit.SECONDS));
		assertEquals("pong", actual[0]);
	}

	@Test
	public void testExecute3() throws Exception {
		final String[] actual = new String[1];
		final CountDownLatch latch = new CountDownLatch(1);
		System.out.println("**** " + Thread.currentThread().getName());
		this.target.execute(new RiakAction<OP>() {
			@Override
			public void execute(OP operations) {
				System.out.println("++++ " + Thread.currentThread().getName());
				RiakFuture waiter = operations
						.ping(new TestingHandler<String>() {
							@Override
							public void handle(
									RiakContentsResponse<String> response)
									throws Exception {
								System.out.println("---- "
										+ Thread.currentThread().getName());
								actual[0] = response.getContents();
							}
						});
				try {
					assertTrue("test is timeout.",
							waiter.await(3, TimeUnit.SECONDS));
					latch.countDown();
				} catch (InterruptedException e) {
					fail(e.getMessage());
				}
			}
		});
		assertTrue("test is timeout.", latch.await(3, TimeUnit.SECONDS));
		assertEquals("pong", actual[0]);
	}
}
