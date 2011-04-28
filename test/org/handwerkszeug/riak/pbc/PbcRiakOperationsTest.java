package org.handwerkszeug.riak.pbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.handwerkszeug.riak.Hosts;
import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.op.RiakResponse;
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
	public void testGet() throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);

		final boolean[] is = { false };
		this.target.get(new Location("hb", "first"),
				new RiakResponseHandler<RiakObject<byte[]>>() {
					@Override
					public void handle(RiakResponse<RiakObject<byte[]>> response)
							throws RiakException {
						try {
							assertFalse(response.isErrorResponse());
							RiakObject<byte[]> content = response.getResponse();
							String hello = new String(content.getContent());
							assertEquals("hello", hello);
							System.out.println(content);
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
