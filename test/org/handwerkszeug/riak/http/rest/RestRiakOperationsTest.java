package org.handwerkszeug.riak.http.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import net.iharder.Base64;

import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.Hosts;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.http.LinkCondition;
import org.handwerkszeug.riak.model.DefaultPutOptions;
import org.handwerkszeug.riak.model.DefaultRiakObject;
import org.handwerkszeug.riak.model.Link;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.PutOptions;
import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.model.RiakResponse;
import org.handwerkszeug.riak.op.RiakOperations;
import org.handwerkszeug.riak.op.RiakOperationsTest;
import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.junit.Test;

public class RestRiakOperationsTest extends RiakOperationsTest {

	RestRiakOperations target;

	@Override
	protected ChannelPipelineFactory newChannelPipelineFactory() {
		return new RestPipelineFactory();
	}

	@Override
	protected RiakOperations newTarget(Channel channel) {
		this.target = new RestRiakOperations(Hosts.RIAK_URL, "riak", channel);
		return this.target;
	}

	@Override
	protected SocketAddress connectTo() {
		return Hosts.RIAK_HTTP_ADDR;
	}

	@Test
	public void testParseLink() throws Exception {
		String link = "</riak/hb/second>; riaktag=\"foo\", </riak/hb/third>; riaktag=\"bar\", </riak/hb>; rel=\"up\"";
		List<String> links = new ArrayList<String>();
		links.add(link);
		List<Link> actual = target.parse(links);

		assertEquals(2, actual.size());
		Link second = actual.get(0);
		assertEquals(new Location("hb", "second"), second.getLocation());
		assertEquals("foo", second.getTag());

		Link third = actual.get(1);
		assertEquals(new Location("hb", "third"), third.getLocation());
		assertEquals("bar", third.getTag());
	}

	@Override
	protected void testSetClientId(String id) throws Exception {
		this.target.setClientId(id);
		assertEquals(id, this.target.getClientId());
	}

	@Test
	public void testPost() throws Exception {
		Location location = new Location("testPost", "");
		String testdata = new Date() + "\n";

		Location returned = testPost(location, testdata);
		testDelete(returned);

		returned = testPostWithReturn(location, testdata);
		testDelete(returned);
	}

	public Location testPost(final Location location, final String testdata)
			throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		RiakObject<byte[]> ro = new DefaultRiakObject(location) {
			@Override
			public byte[] getContent() {
				return testdata.getBytes();
			}
		};
		final Location[] loc = new Location[1];
		target.post(ro, new RiakResponseHandler<RiakObject<byte[]>>() {
			@Override
			public void onError(RiakResponse response) throws Exception {
				waiter.compareAndSet(false, true);
				fail(response.getMessage());
			}

			@Override
			public void handle(RiakContentsResponse<RiakObject<byte[]>> response)
					throws Exception {
				try {
					RiakObject<byte[]> returned = response.getContents();
					assertNotNull(returned.getLocation());
					Location l = returned.getLocation();
					assertEquals(location.getBucket(), l.getBucket());
					assertFalse(l.getKey().isEmpty());
					loc[0] = l;
					is[0] = true;
				} finally {
					waiter.compareAndSet(false, true);
				}
			}
		});

		wait(waiter, is);
		return loc[0];
	}

	public Location testPostWithReturn(final Location location,
			final String testdata) throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		RiakObject<byte[]> ro = new DefaultRiakObject(location) {
			@Override
			public byte[] getContent() {
				return testdata.getBytes();
			}
		};

		PutOptions options = new DefaultPutOptions() {
			@Override
			public boolean getReturnBody() {
				return true;
			}
		};

		final Location[] loc = new Location[1];
		target.post(ro, options, new RiakResponseHandler<RiakObject<byte[]>>() {
			@Override
			public void onError(RiakResponse response) throws Exception {
				waiter.compareAndSet(false, true);
				fail(response.getMessage());
			}

			@Override
			public void handle(RiakContentsResponse<RiakObject<byte[]>> response)
					throws Exception {
				try {
					RiakObject<byte[]> returned = response.getContents();
					assertNotNull(returned.getLocation());
					Location l = returned.getLocation();
					assertEquals(location.getBucket(), l.getBucket());
					assertFalse(l.getKey().isEmpty());
					loc[0] = l;
					is[0] = true;
				} finally {
					waiter.compareAndSet(false, true);
				}
			}
		});

		wait(waiter, is);
		return loc[0];
	}

	@Test
	public void testWalk() throws Exception {

		// A -> B -> D
		// A -> C -> D
		String bucket = "testWalk";
		RiakObject<byte[]> D = createData(bucket, "D", new ArrayList<Link>());
		testDelete(D.getLocation());
		put(D);

		List<Link> c2d = new ArrayList<Link>();
		c2d.add(new Link(D.getLocation(), "2d"));
		RiakObject<byte[]> C = createData(bucket, "C", c2d);
		testDelete(C.getLocation());
		put(C);

		List<Link> b2d = new ArrayList<Link>();
		b2d.add(new Link(D.getLocation(), "2d"));
		RiakObject<byte[]> B = createData(bucket, "B", b2d);
		testDelete(B.getLocation());
		put(B);

		List<Link> a = new ArrayList<Link>();
		a.add(new Link(C.getLocation(), "a2c"));
		a.add(new Link(B.getLocation(), "a2b"));
		RiakObject<byte[]> A = createData(bucket, "A", a);
		testDelete(A.getLocation());
		put(A);

		// walk
		List<List<byte[]>> list = new ArrayList<List<byte[]>>();
		List<byte[]> phase1 = new ArrayList<byte[]>();
		phase1.add(B.getContent());
		phase1.add(C.getContent());
		list.add(phase1);

		List<byte[]> phase2 = new ArrayList<byte[]>();
		phase2.add(D.getContent());
		list.add(phase2);

		testWalk(A.getLocation(), list);
		// delete
	}

	RiakObject<byte[]> createData(String bucket, String key, List<Link> links) {
		Location location = new Location(bucket, key);
		DefaultRiakObject ro = new DefaultRiakObject(location);
		ro.setLinks(links);
		Random r = new Random();
		byte[] bytes = new byte[20];
		r.nextBytes(bytes);
		String data = Base64.encodeBytes(bytes);
		ro.setContent(data.getBytes());
		return ro;
	}

	public void put(RiakObject<byte[]> ro) throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		target.put(ro, new RiakResponseHandler<_>() {
			@Override
			public void onError(RiakResponse response) throws Exception {
				waiter.compareAndSet(false, true);
				fail(response.getMessage());
			}

			@Override
			public void handle(RiakContentsResponse<_> response)
					throws Exception {
				try {
					is[0] = true;
				} finally {
					waiter.compareAndSet(false, true);
				}
			}
		});

		wait(waiter, is);
	}

	public void testWalk(Location init, List<List<byte[]>> expected)
			throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		List<LinkCondition> conds = new ArrayList<LinkCondition>();
		conds.add(LinkCondition.bucket(null, true));
		conds.add(LinkCondition.bucket(null, false));

		final List<List<byte[]>> actual = new ArrayList<List<byte[]>>();
		target.walk(init, conds,
				new RiakResponseHandler<List<RiakObject<byte[]>>>() {
					@Override
					public void onError(RiakResponse response) throws Exception {
						waiter.compareAndSet(false, true);
						fail(response.getMessage());
					}

					@Override
					public void handle(
							RiakContentsResponse<List<RiakObject<byte[]>>> response)
							throws Exception {
						try {
							List<byte[]> list = new ArrayList<byte[]>();
							for (RiakObject<byte[]> ro : response.getContents()) {
								list.add(ro.getContent());
							}
							actual.add(list);
						} finally {
							if (1 < actual.size()) {
								is[0] = true;
								waiter.compareAndSet(false, true);
							}
						}
					}
				});

		wait(waiter, is);
		assertEquals(2, actual.size());

		List<byte[]> e1 = expected.get(0);
		List<byte[]> a1 = actual.get(0);

		assertEquals(new String(e1.get(0)), new String(a1.get(0)));
		assertEquals(new String(e1.get(1)), new String(a1.get(1)));

		List<byte[]> e2 = expected.get(1);
		List<byte[]> a2 = actual.get(1);

		assertEquals(new String(e2.get(0)), new String(a2.get(0)));
	}

	@Test
	public void testGetStats() throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };
		target.getStats(new RiakResponseHandler<ObjectNode>() {
			@Override
			public void onError(RiakResponse response) throws Exception {
				waiter.compareAndSet(false, true);
				fail(response.getMessage());
			}

			@Override
			public void handle(RiakContentsResponse<ObjectNode> response)
					throws Exception {
				try {
					assertNotNull(response.getContents());
					ObjectNode node = response.getContents();
					assertNotNull(node.get("riak_kv_version"));
					System.out.println(node);

					is[0] = true;
				} finally {
					waiter.compareAndSet(false, true);
				}
			}
		});
		wait(waiter, is);
	}
}
