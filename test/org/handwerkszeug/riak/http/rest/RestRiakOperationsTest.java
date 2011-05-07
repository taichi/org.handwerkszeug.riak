package org.handwerkszeug.riak.http.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import net.iharder.Base64;

import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.Config;
import org.handwerkszeug.riak.Hosts;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.config.DefaultConfig;
import org.handwerkszeug.riak.http.InputStreamHandler;
import org.handwerkszeug.riak.http.LinkCondition;
import org.handwerkszeug.riak.http.StreamResponseHandler;
import org.handwerkszeug.riak.model.AbstractRiakObject;
import org.handwerkszeug.riak.model.DefaultPutOptions;
import org.handwerkszeug.riak.model.DefaultRiakObject;
import org.handwerkszeug.riak.model.KeyResponse;
import org.handwerkszeug.riak.model.Link;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.PutOptions;
import org.handwerkszeug.riak.model.Range;
import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.model.RiakResponse;
import org.handwerkszeug.riak.op.RiakOperations;
import org.handwerkszeug.riak.op.RiakOperationsTest;
import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.junit.Test;

/**
 * @author taichi
 */
public class RestRiakOperationsTest extends RiakOperationsTest {

	static final Config config = DefaultConfig.newConfig(Hosts.RIAK_HOST,
			Hosts.RIAK_HTTP_PORT);

	RestRiakOperations target;

	@Override
	protected ChannelPipelineFactory newChannelPipelineFactory() {
		return new RestPipelineFactory();
	}

	@Override
	protected RiakOperations newTarget(Channel channel) {

		this.target = new RestRiakOperations(RestRiakClient.toRiakURI(config),
				config, channel);
		return this.target;
	}

	@Override
	protected SocketAddress connectTo() {
		return config.getRiakAddress();
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
		put(D);

		List<Link> c2d = new ArrayList<Link>();
		c2d.add(new Link(D.getLocation(), "2d"));
		RiakObject<byte[]> C = createData(bucket, "C", c2d);
		put(C);

		List<Link> b2d = new ArrayList<Link>();
		b2d.add(new Link(D.getLocation(), "2d"));
		RiakObject<byte[]> B = createData(bucket, "B", b2d);
		put(B);

		List<Link> a = new ArrayList<Link>();
		a.add(new Link(C.getLocation(), "a2c"));
		a.add(new Link(B.getLocation(), "a2b"));
		RiakObject<byte[]> A = createData(bucket, "A", a);
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
		try {
			testWalk(A.getLocation(), list);
		} finally {
			// delete
			testDelete(D.getLocation());
			testDelete(C.getLocation());
			testDelete(B.getLocation());
			testDelete(A.getLocation());
		}
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

	static final String LARGEFILE = "org/handwerkszeug/riak/http/rest/large_data.jpg";

	@Test
	public void testDeleteAllFromLuwak() throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { true };

		final List<String> list = new ArrayList<String>();
		target.listKeys("luwak_tld", new RiakResponseHandler<KeyResponse>() {
			@Override
			public void onError(RiakResponse response) throws Exception {
				System.err.println(response.getMessage());
				waiter.compareAndSet(false, true);
			}

			@Override
			public void handle(RiakContentsResponse<KeyResponse> response)
					throws Exception {
				KeyResponse keys = response.getContents();
				for (String s : keys.getKeys()) {
					list.add(s);
				}
				if (keys.getDone()) {
					waiter.compareAndSet(false, true);
				}
			}
		});

		wait(waiter, is);

		for (String s : list) {
			testDeleteFromLuwak(s);
		}
	}

	@Test
	public void testLuwak() throws Exception {
		for (int i = 0; i < 1; i++) {
			String key = testPostToLuwak();
			try {
				System.out.println("luwak storaging wait.");
				Thread.sleep(150);
				testGetStream(key);
				testGetRangeStream(key);
			} finally {
				testDeleteFromLuwak(key);
				System.err.println(i);
			}
		}
	}

	public String testPostToLuwak() throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		URL url = getClass().getClassLoader().getResource(LARGEFILE);
		final File file = new File(url.getFile());
		final String[] key = new String[1];
		RiakObject<InputStreamHandler> ro = new AbstractRiakObject<InputStreamHandler>() {
			@Override
			public InputStreamHandler getContent() {
				return new InputStreamHandler() {

					@Override
					public InputStream open() throws IOException {
						return new BufferedInputStream(
								new FileInputStream(file));
					}

					@Override
					public long getContentLength() {
						return file.length();
					}
				};
			}
		};
		ro.setContentType("image/jpeg");
		Map<String, String> map = new HashMap<String, String>();
		map.put("Mmm", "ZZZZZ");
		ro.setUserMetadata(map);

		// link is erased by luwak.
		List<Link> links = new ArrayList<Link>();
		links.add(new Link(new Location("bbb", "kkk"), "ttt"));
		ro.setLinks(links);

		target.postStream(ro, new RiakResponseHandler<String>() {

			@Override
			public void onError(RiakResponse response) throws Exception {
				waiter.compareAndSet(false, true);
				fail(response.getMessage());
			}

			@Override
			public void handle(RiakContentsResponse<String> response)
					throws Exception {
				try {
					key[0] = response.getContents();
					assertNotNull(key[0]);
					is[0] = true;
				} finally {
					waiter.compareAndSet(false, true);
				}
			}
		});

		wait(waiter, is);
		return key[0];
	}

	public void testGetStream(String key) throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		final File download = new File("bin/download.jpg");
		if (download.exists()) {
			download.delete();
		}

		target.getStream(key, new StreamResponseHandler() {

			FileOutputStream out;

			@Override
			public void onError(RiakResponse response) throws Exception {
				waiter.compareAndSet(false, true);
				fail(response.getMessage());
			}

			@Override
			public void begin(RiakObject<_> header) throws Exception {
				Map<String, String> map = header.getUserMetadata();
				assertEquals("ZZZZZ", map.get("Mmm"));
				out = new FileOutputStream(download);
			}

			@Override
			public void handle(RiakContentsResponse<ChannelBuffer> response)
					throws Exception {
				System.out.println("receive chunk...");
				ChannelBuffer buffer = response.getContents();
				out.write(buffer.array());
				out.flush();
			}

			@Override
			public void end() throws Exception {
				System.out.println("END***********");
				out.close();
				is[0] = true;
				waiter.compareAndSet(false, true);
			}

		});
		wait(waiter, is);

		URL url = getClass().getClassLoader().getResource(LARGEFILE);
		final File file = new File(url.getFile());
		assertEquals("length", file.length(), download.length());
	}

	public void testGetRangeStream(String key) throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		final File download = new File("bin/download.jpg.part");
		if (download.exists()) {
			download.delete();
		}

		Range range = Range.ranges(Range.range(101, 10000),
				Range.range(10101, 10200));
		target.getStream(key, range, new StreamResponseHandler() {

			FileOutputStream out;

			@Override
			public void onError(RiakResponse response) throws Exception {
				waiter.compareAndSet(false, true);
				fail(response.getMessage());
			}

			@Override
			public void begin(RiakObject<_> header) throws Exception {
				Map<String, String> map = header.getUserMetadata();
				assertEquals("ZZZZZ", map.get("Mmm"));
				out = new FileOutputStream(download);
			}

			@Override
			public void handle(RiakContentsResponse<ChannelBuffer> response)
					throws Exception {
				ChannelBuffer buffer = response.getContents();
				out.write(buffer.array());
				out.flush();
			}

			@Override
			public void end() throws Exception {
				out.close();
				is[0] = true;
				waiter.compareAndSet(false, true);
			}

		});
		wait(waiter, is);

		assertEquals("length", 10000, download.length());
	}

	public void testDeleteFromLuwak(String key) throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		target.delete(key, new RiakResponseHandler<_>() {
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

	@Test
	public void testPutToLuwak() throws Exception {
		String key = testPostToLuwak();
		Thread.sleep(150);
		testPutToLuwak(key);
		testDeleteFromLuwak(key);
	}

	public void testPutToLuwak(String key) throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		URL url = getClass().getClassLoader().getResource(LARGEFILE);
		final File file = new File(url.getFile());
		Location location = new Location("", key);
		RiakObject<InputStreamHandler> ro = new AbstractRiakObject<InputStreamHandler>(
				location) {
			@Override
			public InputStreamHandler getContent() {
				return new InputStreamHandler() {

					@Override
					public InputStream open() throws IOException {
						return new BufferedInputStream(
								new FileInputStream(file));
					}

					@Override
					public long getContentLength() {
						return file.length();
					}
				};
			}
		};
		ro.setContentType("image/jpeg");
		Map<String, String> map = new HashMap<String, String>();
		map.put("Mmm", "XXXXX");
		ro.setUserMetadata(map);

		target.putStream(ro, new RiakResponseHandler<_>() {

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
}
