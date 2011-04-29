package org.handwerkszeug.riak.pbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.handwerkszeug.riak.Hosts;
import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.model.DefaultRiakObject;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.model.RiakResponse;
import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PbcRiakOperationsTest {

	ClientBootstrap bootstrap;
	Channel channel;
	PbcRiakOperations target;

	@Before
	public void setUp() throws Exception {
		this.bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));
		this.bootstrap.setPipelineFactory(new PbcPipelineFactory());
		ChannelFuture future = this.bootstrap.connect(Hosts.RIAK_ADDR);
		this.channel = future.awaitUninterruptibly().getChannel();

		this.target = new PbcRiakOperations(this.channel);
	}

	@After
	public void tearDown() throws Exception {
		this.channel.close().awaitUninterruptibly();
		this.bootstrap.releaseExternalResources();
	}

	@Test
	public void testPing() throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);

		final boolean[] is = { false };
		this.target.ping(new RiakResponseHandler<_>() {
			@Override
			public void handle(RiakResponse<_> response) throws RiakException {
				try {
					assertFalse(response.isErrorResponse());
					assertEquals("pong", response.getMessage());
					is[0] = true;
				} finally {
					waiter.compareAndSet(false, true);
				}
			}
		});

		wait(waiter, is);
	}

	@Test
	public void testPutGetDel() throws Exception {
		Location location = new Location("testBucket", "testKey");
		String testdata = new SimpleDateFormat().format(new Date()) + "\n";
		testPut(location, testdata);
		testGet(location, testdata);
		testDelete(location);
	}

	public void testGet(Location location, final String testdata)
			throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		this.target.get(location,
				new RiakResponseHandler<RiakObject<byte[]>>() {
					@Override
					public void handle(RiakResponse<RiakObject<byte[]>> response)
							throws RiakException {
						try {
							assertFalse(response.isErrorResponse());
							RiakObject<byte[]> content = response.getResponse();
							String actual = new String(content.getContent());
							assertEquals(testdata, actual);
							is[0] = true;
						} finally {
							waiter.compareAndSet(false, true);
						}
					}
				});

		wait(waiter, is);
	}

	public void testPut(Location location, final String testdata)
			throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);

		RiakObject<byte[]> ro = new DefaultRiakObject(location) {
			@Override
			public byte[] getContent() {
				return testdata.getBytes();
			}
		};
		final boolean[] is = { false };
		this.target.put(ro,
				new RiakResponseHandler<List<RiakObject<byte[]>>>() {
					@Override
					public void handle(
							RiakResponse<List<RiakObject<byte[]>>> response)
							throws RiakException {
						try {
							assertFalse(response.isErrorResponse());
							assertEquals(0, response.getResponse().size());
							is[0] = true;
						} finally {
							waiter.compareAndSet(false, true);
						}
					}
				});

		wait(waiter, is);
	}

	public void testDelete(Location location) throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		target.delete(location, new RiakResponseHandler<_>() {
			@Override
			public void handle(RiakResponse<_> response) throws RiakException {
				try {
					assertFalse(response.isErrorResponse());
					is[0] = true;
				} finally {
					waiter.compareAndSet(false, true);
				}
			}
		});

		wait(waiter, is);
	}

	@Test
	public void testClientId() throws Exception {
		String id = "AXZH";
		testSetClientId(id);
		testGetClientId(id);
	}

	public void testSetClientId(String id) throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		this.target.setClientId(id, new RiakResponseHandler<_>() {
			@Override
			public void handle(RiakResponse<_> response) throws RiakException {
				try {
					assertFalse(response.isErrorResponse());
					is[0] = true;
				} finally {
					waiter.compareAndSet(false, true);
				}
			}
		});

		wait(waiter, is);
	}

	public void testGetClientId(final String id) throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		this.target.getClientId(new RiakResponseHandler<String>() {
			@Override
			public void handle(RiakResponse<String> response)
					throws RiakException {
				try {
					assertEquals(id, response.getResponse());
					is[0] = true;
				} finally {
					waiter.compareAndSet(false, true);
				}
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
