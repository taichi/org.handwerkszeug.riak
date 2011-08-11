package org.handwerkszeug.riak.transport.protobuf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.SocketAddress;

import junit.framework.Assert;

import org.handwerkszeug.riak.Hosts;
import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.model.RiakFuture;
import org.handwerkszeug.riak.model.ServerInfo;
import org.handwerkszeug.riak.op.RiakOperationsTest;
import org.handwerkszeug.riak.op.TestingHandler;
import org.handwerkszeug.riak.transport.protobuf.internal.ProtoBufPipelineFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author taichi
 */
public class ProtoBufRiakOperationsTest extends
		RiakOperationsTest<ProtoBufRiakOperations> {

	static final ProtoBufRiakConfig config = ProtoBufRiakConfig.newConfig(
			Hosts.RIAK_HOST, Hosts.RIAK_PB_PORT);

	@Override
	protected ChannelPipelineFactory newChannelPipelineFactory() {
		return new ProtoBufPipelineFactory();
	}

	@Override
	protected SocketAddress connectTo() {
		return config.getRiakAddress();
	}

	@Override
	protected ProtoBufRiakOperations newTarget(Channel channel) {
		return new ProtoBufRiakOperations(channel);
	}

	@Test
	public void testClientId() throws Exception {
		String id = "AXZH";
		testSetClientId(id);
		testGetClientId(id);

		try {
			this.target.setClientId("12345", new TestingHandler<_>() {
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
		final boolean[] is = { false };

		RiakFuture waiter = this.target.setClientId(id,
				new TestingHandler<_>() {
					@Override
					public void handle(RiakContentsResponse<_> response)
							throws RiakException {
						is[0] = true;
					}
				});

		waitFor(waiter);
		assertTrue(is[0]);
	}

	public void testGetClientId(final String id) throws Exception {
		final String[] actual = new String[1];

		RiakFuture waiter = this.target
				.getClientId(new TestingHandler<String>() {
					@Override
					public void handle(RiakContentsResponse<String> response)
							throws RiakException {
						actual[0] = response.getContents();
					}

				});

		waitFor(waiter);
		assertEquals(actual[0], id);
	}

	@Test
	public void testGetServerInfo() throws Exception {
		final ServerInfo[] actual = new ServerInfo[1];
		RiakFuture waiter = this.target
				.getServerInfo(new TestingHandler<ServerInfo>() {

					@Override
					public void handle(RiakContentsResponse<ServerInfo> response)
							throws RiakException {
						actual[0] = response.getContents();
					}
				});
		waitFor(waiter);
		assertNotNull(actual[0]);
		assertNotNull(actual[0].getNode());
		assertNotNull(actual[0].getServerVersion());
	}

	@Override
	@Test
	@Ignore("Riak 0.14.2 has bug. fix that bug in the future")
	public void testPost() throws Exception {
		super.testPost();
	}
}
