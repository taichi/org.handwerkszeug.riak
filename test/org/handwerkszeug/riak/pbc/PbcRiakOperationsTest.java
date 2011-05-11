package org.handwerkszeug.riak.pbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.SocketAddress;
import java.util.concurrent.CountDownLatch;

import junit.framework.Assert;

import org.handwerkszeug.riak.Config;
import org.handwerkszeug.riak.Hosts;
import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.config.DefaultConfig;
import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.model.RiakResponse;
import org.handwerkszeug.riak.model.ServerInfo;
import org.handwerkszeug.riak.op.RiakOperations;
import org.handwerkszeug.riak.op.RiakOperationsTest;
import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.junit.Test;

/**
 * @author taichi
 */
public class PbcRiakOperationsTest extends RiakOperationsTest {

	static final Config config = DefaultConfig.newConfig(Hosts.RIAK_HOST,
			Hosts.RIAK_PB_PORT);

	PbcRiakOperations target;

	@Override
	protected ChannelPipelineFactory newChannelPipelineFactory() {
		return new PbcPipelineFactory();
	}

	@Override
	protected SocketAddress connectTo() {
		return config.getRiakAddress();
	}

	@Override
	protected RiakOperations newTarget(Channel channel) {
		this.target = new PbcRiakOperations(channel);
		return this.target;
	}

	@Test
	public void testClientId() throws Exception {
		String id = "AXZH";
		testSetClientId(id);
		testGetClientId(id);

		try {
			this.target.setClientId("12345", new RiakResponseHandler<_>() {
				@Override
				public void onError(RiakResponse response) throws RiakException {
				}

				@Override
				public void handle(RiakContentsResponse<_> response)
						throws RiakException {
				}
			});
			Assert.fail();
		} catch (IllegalArgumentException e) {
			assertTrue(true);
		}
	}

	@Override
	public void testSetClientId(String id) throws Exception {
		final CountDownLatch waiter = new CountDownLatch(1);
		final boolean[] is = { false };

		this.target.setClientId(id, new RiakResponseHandler<_>() {
			@Override
			public void onError(RiakResponse response) throws RiakException {
				waiter.countDown();
			}

			@Override
			public void handle(RiakContentsResponse<_> response)
					throws RiakException {
				is[0] = true;
				waiter.countDown();
			}
		});

		wait(waiter, is);
	}

	public void testGetClientId(final String id) throws Exception {
		final CountDownLatch waiter = new CountDownLatch(1);
		final boolean[] is = { false };
		final String[] act = new String[1];

		this.target.getClientId(new RiakResponseHandler<String>() {
			@Override
			public void onError(RiakResponse response) throws RiakException {
				waiter.countDown();
				fail(response.getMessage());
			}

			@Override
			public void handle(RiakContentsResponse<String> response)
					throws RiakException {
				is[0] = true;
				act[0] = response.getContents();
				waiter.countDown();
			}

		});

		wait(waiter, is);
		assertEquals(act[0], id);
	}

	@Test
	public void testGetServerInfo() throws Exception {
		final CountDownLatch waiter = new CountDownLatch(1);
		final boolean[] is = { false };

		this.target.getServerInfo(new RiakResponseHandler<ServerInfo>() {
			@Override
			public void onError(RiakResponse response) throws RiakException {
				waiter.countDown();
				fail(response.getMessage());
			}

			@Override
			public void handle(RiakContentsResponse<ServerInfo> response)
					throws RiakException {
				try {
					ServerInfo info = response.getContents();
					assertNotNull(info.getNode());
					assertNotNull(info.getServerVersion());
					is[0] = true;
				} finally {
					waiter.countDown();
				}
			}
		});
		wait(waiter, is);
	}

}
