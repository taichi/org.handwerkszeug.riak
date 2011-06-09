package org.handwerkszeug.riak.transport.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import net.iharder.Base64;

import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.Hosts;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.model.AbstractRiakObject;
import org.handwerkszeug.riak.model.DefaultRiakObject;
import org.handwerkszeug.riak.model.KeyResponse;
import org.handwerkszeug.riak.model.Link;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.Range;
import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.model.RiakFuture;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.model.RiakResponse;
import org.handwerkszeug.riak.op.RiakOperations;
import org.handwerkszeug.riak.op.RiakOperationsTest;
import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.handwerkszeug.riak.transport.internal.DefaultCompletionChannelHandler;
import org.handwerkszeug.riak.transport.rest.internal.RestPipelineFactory;
import org.handwerkszeug.riak.util.LogbackUtil;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.junit.Test;

/**
 * @author taichi
 */
public class RestRiakOperationsTest extends RiakOperationsTest {

	static final RestRiakConfig config = RestRiakConfig.newConfig(
			Hosts.RIAK_HOST, Hosts.RIAK_HTTP_PORT);

	public RestRiakOperations target;

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

	@Override
	protected void testSetClientId(String id) throws Exception {
		this.target.setClientId(id);
		assertEquals(id, this.target.getClientId());
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
		final boolean[] is = { false };

		RiakFuture waiter = this.target.put(ro, new RiakResponseHandler<_>() {
			@Override
			public void onError(RiakResponse response) throws Exception {
				fail(response.getMessage());
			}

			@Override
			public void handle(RiakContentsResponse<_> response)
					throws Exception {
				is[0] = true;
			}
		});

		wait(waiter, is);
	}

	public void testWalk(Location init, List<List<byte[]>> expected)
			throws Exception {
		final boolean[] is = { false };

		List<LinkCondition> conds = new ArrayList<LinkCondition>();
		conds.add(LinkCondition.bucket(null, true));
		conds.add(LinkCondition.bucket(null, false));

		final List<List<byte[]>> actual = new ArrayList<List<byte[]>>();
		RiakFuture waiter = this.target.walk(init, conds,
				new RiakResponseHandler<List<RiakObject<byte[]>>>() {
					@Override
					public void onError(RiakResponse response) throws Exception {
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
							}
						}
					}
				});

		wait(waiter, is);
		assertEquals(2, actual.size());

		List<String> e1 = new ArrayList<String>();
		for (byte[] b : expected.get(0)) {
			e1.add(new String(b));
		}
		List<String> a1 = new ArrayList<String>();
		for (byte[] b : actual.get(0)) {
			a1.add(new String(b));
		}

		System.out.println(e1);
		System.out.println(a1);
		assertTrue(e1.contains(a1.get(0)));
		assertTrue(e1.contains(a1.get(1)));

		List<byte[]> e2 = expected.get(1);
		List<byte[]> a2 = actual.get(1);

		assertEquals(new String(e2.get(0)), new String(a2.get(0)));
	}

	@Test
	public void testGetStats() throws Exception {
		final boolean[] is = { false };
		RiakFuture waiter = this.target
				.getStats(new RiakResponseHandler<ObjectNode>() {
					@Override
					public void onError(RiakResponse response) throws Exception {
						fail(response.getMessage());
					}

					@Override
					public void handle(RiakContentsResponse<ObjectNode> response)
							throws Exception {
						assertNotNull(response.getContents());
						ObjectNode node = response.getContents();
						assertNotNull(node.get("riak_kv_version"));
						System.out.println(node);

						is[0] = true;
					}
				});
		wait(waiter, is);
	}

	static final String LARGEFILE = "org/handwerkszeug/riak/transport/rest/large_data.jpg";

	@Test
	public void testDeleteAllFromLuwak() throws Exception {
		final boolean[] is = { true };

		final List<String> list = new ArrayList<String>();
		RiakFuture waiter = this.target.listKeys("luwak_tld",
				new RiakResponseHandler<KeyResponse>() {
					@Override
					public void onError(RiakResponse response) throws Exception {
						fail(response.getMessage());
					}

					@Override
					public void handle(
							RiakContentsResponse<KeyResponse> response)
							throws Exception {
						KeyResponse keys = response.getContents();
						for (String s : keys.getKeys()) {
							list.add(s);
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
		// for slow test problem.
		// sibling message body is huge.
		LogbackUtil.suppressLogging(new LogbackUtil.Action() {
			@Override
			public void execute() throws Exception {
				for (int i = 0; i < 5; i++) {

					String key = testPostToLuwak();
					try {
						System.out.println("luwak storaging wait.");
						Thread.sleep(300);
						testGetStream(key);
						testGetRangeStream(key);
					} finally {
						testDeleteFromLuwak(key);
						System.err.println(i);
					}
				}
			}
		}, DefaultCompletionChannelHandler.class);
	}

	public String testPostToLuwak() throws Exception {
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

		RiakFuture waiter = this.target.postStream(ro,
				new RiakResponseHandler<String>() {

					@Override
					public void onError(RiakResponse response) throws Exception {
						fail(response.getMessage());
					}

					@Override
					public void handle(RiakContentsResponse<String> response)
							throws Exception {
						key[0] = response.getContents();
						assertNotNull(key[0]);
						is[0] = true;
					}
				});

		wait(waiter, is);
		return key[0];
	}

	public void testGetStream(String key) throws Exception {
		final boolean[] is = { false };

		final File download = new File("target/test-classes/download.jpg");
		if (download.exists()) {
			download.delete();
		}

		RiakFuture waiter = this.target.getStream(key,
				new StreamResponseHandler() {

					FileOutputStream out;

					@Override
					public void onError(RiakResponse response) throws Exception {
						fail(response.getMessage());
					}

					@Override
					public void begin(RiakObject<_> header) throws Exception {
						Map<String, String> map = header.getUserMetadata();
						assertEquals("ZZZZZ", map.get("Mmm"));
						this.out = new FileOutputStream(download);
					}

					@Override
					public void handle(
							RiakContentsResponse<ChannelBuffer> response)
							throws Exception {
						System.out.println("receive chunk...");
						ChannelBuffer buffer = response.getContents();
						this.out.write(buffer.array());
						this.out.flush();
					}

					@Override
					public void end() throws Exception {
						System.out.println("END***********");
						this.out.close();
						is[0] = true;
					}

				});
		assertTrue("timeout", waiter.await(5, TimeUnit.SECONDS));
		assertTrue(is[0]);
		URL url = getClass().getClassLoader().getResource(LARGEFILE);
		final File file = new File(url.getFile());
		assertEquals("length", file.length(), download.length());
	}

	public void testGetRangeStream(String key) throws Exception {
		final boolean[] is = { false };

		final File download = new File("target/test-classes/download.jpg.part");
		if (download.exists()) {
			download.delete();
		}

		Range range = Range.ranges(Range.range(101, 10000),
				Range.range(10101, 10200));
		RiakFuture waiter = this.target.getStream(key, range,
				new StreamResponseHandler() {

					FileOutputStream out;

					@Override
					public void onError(RiakResponse response) throws Exception {
						fail(response.getMessage());
					}

					@Override
					public void begin(RiakObject<_> header) throws Exception {
						Map<String, String> map = header.getUserMetadata();
						assertEquals("ZZZZZ", map.get("Mmm"));
						this.out = new FileOutputStream(download);
					}

					@Override
					public void handle(
							RiakContentsResponse<ChannelBuffer> response)
							throws Exception {
						ChannelBuffer buffer = response.getContents();
						this.out.write(buffer.array());
						this.out.flush();
					}

					@Override
					public void end() throws Exception {
						System.out.println("end");
						this.out.close();
						is[0] = true;
					}

				});
		wait(waiter, is);

		assertEquals("length", 10000, download.length());
	}

	public void testDeleteFromLuwak(String key) throws Exception {
		final boolean[] is = { false };

		RiakFuture waiter = this.target.delete(key,
				new RiakResponseHandler<_>() {
					@Override
					public void onError(RiakResponse response) throws Exception {
						fail(response.getMessage());
					}

					@Override
					public void handle(RiakContentsResponse<_> response)
							throws Exception {
						is[0] = true;
					}
				});

		wait(waiter, is);
	}

	@Test
	public void testPutToLuwak() throws Exception {
		String key = testPostToLuwak();
		Thread.sleep(150);
		testPutToLuwak(key);
		Thread.sleep(150);
		testDeleteFromLuwak(key);
	}

	public void testPutToLuwak(String key) throws Exception {
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

		RiakFuture waiter = this.target.putStream(ro,
				new RiakResponseHandler<_>() {

					@Override
					public void onError(RiakResponse response) throws Exception {
						fail(response.getMessage());
					}

					@Override
					public void handle(RiakContentsResponse<_> response)
							throws Exception {
						is[0] = true;
					}
				});

		wait(waiter, is);
	}
}
