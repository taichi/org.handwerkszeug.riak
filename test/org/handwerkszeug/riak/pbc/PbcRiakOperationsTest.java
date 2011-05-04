package org.handwerkszeug.riak.pbc;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;

import org.handwerkszeug.riak.Hosts;
import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak._;
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

	PbcRiakOperations target;

	@Override
	protected ChannelPipelineFactory newChannelPipelineFactory() {
		return new PbcPipelineFactory();
	}

	@Override
	protected SocketAddress connectTo() {
		return Hosts.RIAK_PB_ADDR;
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
			target.setClientId("12345", new RiakResponseHandler<_>() {
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

	public void testSetClientId(String id) throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		this.target.setClientId(id, new RiakResponseHandler<_>() {
			@Override
			public void onError(RiakResponse response) throws RiakException {
				waiter.compareAndSet(false, true);
			}

			@Override
			public void handle(RiakContentsResponse<_> response)
					throws RiakException {
				is[0] = true;
				waiter.compareAndSet(false, true);
			}
		});

		wait(waiter, is);
	}

	public void testGetClientId(final String id) throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		this.target.getClientId(new RiakResponseHandler<String>() {
			@Override
			public void onError(RiakResponse response) throws RiakException {
				waiter.compareAndSet(false, true);
			}

			@Override
			public void handle(RiakContentsResponse<String> response)
					throws RiakException {
				is[0] = true;
				waiter.compareAndSet(false, true);
			}

		});

		wait(waiter, is);
	}

	@Test
	public void testGetServerInfo() throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		this.target.getServerInfo(new RiakResponseHandler<ServerInfo>() {
			@Override
			public void onError(RiakResponse response) throws RiakException {
				waiter.compareAndSet(false, true);
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
					waiter.compareAndSet(false, true);
				}
			}
		});
		wait(waiter, is);
	}

}
