package org.handwerkszeug.riak.transport.protobuf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.SocketAddress;

import junit.framework.Assert;

import org.handwerkszeug.riak.Hosts;
import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.model.RiakFuture;
import org.handwerkszeug.riak.model.RiakResponse;
import org.handwerkszeug.riak.model.ServerInfo;
import org.handwerkszeug.riak.op.RiakOperations;
import org.handwerkszeug.riak.op.RiakOperationsTest;
import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.handwerkszeug.riak.transport.protobuf.ProtoBufRiakConfig;
import org.handwerkszeug.riak.transport.protobuf.ProtoBufRiakOperations;
import org.handwerkszeug.riak.transport.protobuf.internal.ProtoBufPipelineFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author taichi
 */
public class ProtoBufRiakOperationsTest extends RiakOperationsTest {

	static final ProtoBufRiakConfig config = ProtoBufRiakConfig.newConfig(Hosts.RIAK_HOST,
			Hosts.RIAK_PB_PORT);

	ProtoBufRiakOperations target;

	@Override
	protected ChannelPipelineFactory newChannelPipelineFactory() {
		return new ProtoBufPipelineFactory();
	}

	@Override
	protected SocketAddress connectTo() {
		return config.getRiakAddress();
	}

	@Override
	protected RiakOperations newTarget(Channel channel) {
		this.target = new ProtoBufRiakOperations(channel);
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
		final boolean[] is = { false };

		RiakFuture waiter = this.target.setClientId(id,
				new RiakResponseHandler<_>() {
					@Override
					public void onError(RiakResponse response)
							throws RiakException {
						fail(response.getMessage());
					}

					@Override
					public void handle(RiakContentsResponse<_> response)
							throws RiakException {
						is[0] = true;
					}
				});

		wait(waiter, is);
	}

	public void testGetClientId(final String id) throws Exception {
		final boolean[] is = { false };
		final String[] act = new String[1];

		RiakFuture waiter = this.target
				.getClientId(new RiakResponseHandler<String>() {
					@Override
					public void onError(RiakResponse response)
							throws RiakException {
						fail(response.getMessage());
					}

					@Override
					public void handle(RiakContentsResponse<String> response)
							throws RiakException {
						is[0] = true;
						act[0] = response.getContents();
					}

				});

		wait(waiter, is);
		assertEquals(act[0], id);
	}

	@Test
	public void testGetServerInfo() throws Exception {
		final boolean[] is = { false };

		RiakFuture waiter = this.target
				.getServerInfo(new RiakResponseHandler<ServerInfo>() {
					@Override
					public void onError(RiakResponse response)
							throws RiakException {
						fail(response.getMessage());
					}

					@Override
					public void handle(RiakContentsResponse<ServerInfo> response)
							throws RiakException {
						ServerInfo info = response.getContents();
						assertNotNull(info.getNode());
						assertNotNull(info.getServerVersion());
						is[0] = true;
					}
				});
		wait(waiter, is);
	}

	@Override
	@Test
	@Ignore("Riak 0.14.1 has bug. 0.14.2 fix that bug.")
	public void testPost() throws Exception {
		super.testPost();
	}
}
