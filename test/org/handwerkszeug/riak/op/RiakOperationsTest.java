package org.handwerkszeug.riak.op;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.mapreduce.JavaScriptPhase;
import org.handwerkszeug.riak.mapreduce.MapReduceInputs;
import org.handwerkszeug.riak.mapreduce.MapReduceKeyFilters;
import org.handwerkszeug.riak.mapreduce.MapReduceQuery;
import org.handwerkszeug.riak.mapreduce.MapReduceQueryConstructor;
import org.handwerkszeug.riak.mapreduce.MapReduceResponse;
import org.handwerkszeug.riak.mapreduce.NamedFunctionPhase;
import org.handwerkszeug.riak.model.Bucket;
import org.handwerkszeug.riak.model.DefaultGetOptions;
import org.handwerkszeug.riak.model.DefaultPostOptions;
import org.handwerkszeug.riak.model.DefaultPutOptions;
import org.handwerkszeug.riak.model.DefaultRiakObject;
import org.handwerkszeug.riak.model.Erlang;
import org.handwerkszeug.riak.model.JavaScript;
import org.handwerkszeug.riak.model.KeyResponse;
import org.handwerkszeug.riak.model.Link;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.PostOptions;
import org.handwerkszeug.riak.model.Quorum;
import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.model.RiakFuture;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.model.RiakResponse;
import org.handwerkszeug.riak.transport.internal.DefaultCompletionChannelHandler;
import org.handwerkszeug.riak.transport.protobuf.ProtoBufRiakOperationsTest;
import org.handwerkszeug.riak.util.JsonUtil;
import org.handwerkszeug.riak.util.LogbackUtil;
import org.handwerkszeug.riak.util.Streams;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author taichi
 */
public abstract class RiakOperationsTest {

	ClientBootstrap bootstrap;
	Channel channel;
	RiakOperations target;

	@Before
	public void setUp() throws Exception {
		this.bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));

		this.bootstrap.setPipelineFactory(newChannelPipelineFactory());

		ChannelFuture future = this.bootstrap.connect(connectTo());
		this.channel = future.awaitUninterruptibly().getChannel();
		this.target = newTarget(this.channel);
	}

	protected abstract ChannelPipelineFactory newChannelPipelineFactory();

	protected abstract SocketAddress connectTo();

	protected abstract RiakOperations newTarget(Channel channel);

	@After
	public void tearDown() throws Exception {
		this.channel.close().awaitUninterruptibly();
		this.bootstrap.releaseExternalResources();
	}

	@Test
	public void testPing() throws Exception {
		final String[] actual = new String[1];
		RiakFuture waiter = this.target.ping(new TestingHandler<String>() {
			@Override
			public void handle(RiakContentsResponse<String> response)
					throws RiakException {
				actual[0] = response.getContents();
			}
		});

		waitFor(waiter);
		assertEquals("pong", actual[0]);
	}

	@Test
	public void testListBuckets() throws Exception {
		final List<String> actual = new ArrayList<String>();
		RiakFuture waiter = this.target
				.listBuckets(new TestingHandler<List<String>>() {
					@Override
					public void handle(
							RiakContentsResponse<List<String>> response)
							throws RiakException {
						List<String> keys = response.getContents();
						actual.addAll(keys);
					}
				});
		waitFor(waiter);
		assertTrue(0 < actual.size());
	}

	@Test
	public void testListKeys() throws Exception {
		String bucket = "testListKeys";
		String testdata = new SimpleDateFormat().format(new Date()) + "\n";
		try {
			for (int i = 0; i < 20; i++) {
				Location l = new Location(bucket, String.valueOf(i));
				testPut(l, testdata);
			}

			testListKeys(bucket, 20);

		} finally {
			for (int i = 0; i < 20; i++) {
				Location l = new Location(bucket, String.valueOf(i));
				testDelete(l);
			}
		}
	}

	public void testListKeys(String bucket, int expected) throws Exception {
		final boolean[] is = { false };

		final int[] actual = { 0 };
		RiakFuture waiter = this.target.listKeys(bucket,
				new TestingHandler<KeyResponse>() {
					@Override
					public void handle(
							RiakContentsResponse<KeyResponse> response)
							throws RiakException {
						KeyResponse kr = response.getContents();
						List<String> list = kr.getKeys();
						actual[0] += list.size();

						if (kr.getDone()) {
							is[0] = true;
						}
					}
				});

		waitFor(waiter);
		assertEquals(expected, actual[0]);
		assertTrue(is[0]);

	}

	@Test
	public void testBucket() throws Exception {
		Location location = new Location("bucketSetGet", "1");
		String testdata = new SimpleDateFormat().format(new Date()) + "\n";

		try {
			testPut(location, testdata);

			Bucket exp = testBucketGet(location.getBucket());
			testBucketSet(exp);
			Bucket act = testBucketGet(location.getBucket());
			assertEquals(exp.getNumberOfReplicas(), act.getNumberOfReplicas());
			assertEquals(exp.getAllowMulti(), act.getAllowMulti());
		} finally {
			testDelete(location);
		}
	}

	public void testBucketSet(Bucket bucket) throws Exception {
		final boolean[] is = { false };
		bucket.setAllowMulti(true);
		bucket.setNumberOfReplicas(1);

		RiakFuture waiter = this.target.setBucket(bucket,
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

	public Bucket testBucketGet(String bucket) throws Exception {
		final Bucket[] bu = new Bucket[1];

		RiakFuture waiter = this.target.getBucket(bucket,
				new TestingHandler<Bucket>() {
					@Override
					public void handle(RiakContentsResponse<Bucket> response)
							throws RiakException {
						assertNotNull(response.getContents());
						bu[0] = response.getContents();
					}
				});

		waitFor(waiter);
		return bu[0];
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

		final String[] actual = new String[1];
		RiakFuture waiter = this.target.get(location,
				new TestingHandler<RiakObject<byte[]>>() {
					@Override
					public void handle(
							RiakContentsResponse<RiakObject<byte[]>> response)
							throws RiakException {
						RiakObject<byte[]> content = response.getContents();
						actual[0] = new String(content.getContent());
					}
				});

		waitFor(waiter);
		assertEquals(testdata, actual[0]);
	}

	public void testPut(Location location, final String testdata)
			throws Exception {

		DefaultRiakObject ro = new DefaultRiakObject(location);
		ro.setContent(testdata.getBytes());
		ro.setCharset("UTF-8");
		List<Link> links = new ArrayList<Link>();
		links.add(new Link(new Location(location.getBucket(), "tag"), "foo"));
		links.add(new Link(new Location(location.getBucket(), "tag"), "bar"));
		ro.setLinks(links);

		Map<String, String> map = new HashMap<String, String>();
		map.put("mmm", "ddd");
		map.put("nn", "eee");
		map.put("o", "fff");
		ro.setUserMetadata(map);

		final boolean[] is = { false };
		RiakFuture waiter = this.target.put(ro, new TestingHandler<_>() {
			@Override
			public void handle(RiakContentsResponse<_> response)
					throws RiakException {
				is[0] = true;
			}
		});

		waitFor(waiter);
		assertTrue(is[0]);

	}

	public void testDelete(Location location) throws Exception {
		final boolean[] is = { false };

		RiakFuture waiter = this.target.delete(location,
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

	@Test
	public void testBatchDelete() throws Exception {
		final String bucket = "testBatchDelete";
		int records = 10;
		for (int i = 0; i < records; i++) {
			testPut(new Location(bucket, String.valueOf(i)), "a");
		}

		final CountDownLatch latch = new CountDownLatch(records);
		this.target.listKeys(bucket, new TestingHandler<KeyResponse>() {
			@Override
			public void handle(RiakContentsResponse<KeyResponse> response)
					throws Exception {
				KeyResponse kr = response.getContents();
				for (final String s : kr.getKeys()) {
					RiakOperationsTest.this.target.delete(new Location(bucket,
							s), new TestingHandler<_>() {
						@Override
						public void handle(RiakContentsResponse<_> response)
								throws Exception {
							latch.countDown();
						}
					});
				}
			}
		});
		assertTrue("timeout.", latch.await(5, TimeUnit.SECONDS));
	}

	@Test
	public void testGetWithOpt() throws Exception {
		final Location location = new Location("testGetWithOpt", "testKey");

		byte[] data = new byte[1024 * 2];
		Random r = new Random();
		r.nextBytes(data);
		final String testdata = Arrays.toString(data);
		testPut(location, testdata);
		try {
			testGetWithOpt(location, testdata);
		} finally {
			testDelete(location);
		}
	}

	protected void testGetWithOpt(final Location location, final String testdata)
			throws InterruptedException {
		final String[] result = new String[1];
		final Location[] actual = new Location[1];
		RiakFuture waiter = this.target.get(location, new DefaultGetOptions() {
			@Override
			public Quorum getReadQuorum() {
				return Quorum.of(2);
			}
		}, new TestingHandler<RiakObject<byte[]>>() {
			@Override
			public void handle(RiakContentsResponse<RiakObject<byte[]>> response)
					throws Exception {
				RiakObject<byte[]> ro = response.getContents();
				result[0] = new String(ro.getContent());
				actual[0] = ro.getLocation();
			}
		});

		waitFor(waiter);
		assertEquals(testdata, result[0]);
		assertEquals(location, actual[0]);
	}

	@Test
	public void testGetNoContents() throws Exception {
		final boolean[] is = { false };
		Location location = new Location("testGetNoContents", "nocont");

		RiakFuture waiter = this.target.get(location,
				new RiakResponseHandler<RiakObject<byte[]>>() {
					@Override
					public void onError(RiakResponse response)
							throws RiakException {
						assertNotNull(response.getMessage());
						assertFalse(response.getMessage().isEmpty());
						is[0] = true;
					}

					@Override
					public void handle(
							RiakContentsResponse<RiakObject<byte[]>> response)
							throws RiakException {
						fail(response.getMessage());
					}
				});

		waitFor(waiter);
		assertTrue(is[0]);

	}

	protected abstract void testSetClientId(String id) throws Exception;

	@Test
	public void testSibling() throws Exception {
		// for slow test problem.
		// sibling message body is huge.
		LogbackUtil.suppressLogging(new LogbackUtil.Action() {
			@Override
			public void execute() throws Exception {
				final Location location = new Location("testSibling", "testKey");
				testPut(location, "1");
				try {
					Bucket bucket = testBucketGet(location.getBucket());
					bucket.setAllowMulti(true);
					testBucketSet(bucket);

					// remove current entry.
					testDelete(location);

					List<String> testdatas = new ArrayList<String>();
					Random r = new Random();
					byte[] bytes = new byte[1024 * 128];
					r.nextBytes(bytes);
					testdatas.add(Arrays.toString(bytes));
					r.nextBytes(bytes);
					testdatas.add(Arrays.toString(bytes));
					r.nextBytes(bytes);
					testdatas.add(Arrays.toString(bytes));

					testSetClientId("AAAA");
					testPut(location, testdatas.get(0));

					testSetClientId("BBBB");
					testPut(location, testdatas.get(1));

					testSetClientId("CCCC");
					testPutWithSibling(location, testdatas.get(2), testdatas);

					testGetWithSibling(location, testdatas);
				} finally {
					// for CUI manually check.
					// testDelete(location);
				}
			}
		}, DefaultCompletionChannelHandler.class);

	}

	protected void testGetWithSibling(final Location location,
			final List<String> testdatas) throws InterruptedException {
		final boolean[] beginEnd = new boolean[2];

		final List<String> actuals = new ArrayList<String>();
		RiakFuture waiter = this.target.get(location, new DefaultGetOptions(),
				new SiblingHandler() {
					@Override
					public void begin() {
						beginEnd[0] = true;
					}

					@Override
					public void onError(RiakResponse response)
							throws RiakException {
						fail(response.getMessage());
					}

					@Override
					public void handle(
							RiakContentsResponse<RiakObject<byte[]>> response)
							throws RiakException {
						RiakObject<byte[]> ro = response.getContents();
						assertEquals(location, ro.getLocation());
						actuals.add(new String(ro.getContent()));
					}

					@Override
					public void end() {
						beginEnd[1] = true;
					}
				});

		assertTrue("test is timeout.", waiter.await(20, TimeUnit.SECONDS));
		assertEquals(3, actuals.size());
		for (String s : testdatas) {
			assertTrue(s, actuals.contains(s));
		}
		assertTrue("begin", beginEnd[0]);
		assertTrue("end", beginEnd[1]);

	}

	protected void testPutWithSibling(final Location location,
			final String testdata, final List<String> testdatas)
			throws Exception {
		final boolean[] beginEnd = new boolean[2];

		DefaultRiakObject ro = new DefaultRiakObject(location);
		ro.setContent(testdata.getBytes());

		final List<String> actuals = new ArrayList<String>();
		RiakFuture waiter = this.target.put(ro, new DefaultPutOptions() {
			@Override
			public boolean getReturnBody() {
				return true;
			}
		}, new SiblingHandler() {

			@Override
			public void onError(RiakResponse response) throws Exception {
				fail(response.getMessage());
			}

			@Override
			public void begin() {
				beginEnd[0] = true;
			}

			@Override
			public void handle(RiakContentsResponse<RiakObject<byte[]>> response)
					throws Exception {
				RiakObject<byte[]> ro = response.getContents();
				assertEquals(location, ro.getLocation());
				actuals.add(new String(ro.getContent()));
			}

			@Override
			public void end() {
				beginEnd[1] = true;
			}

		});
		assertTrue("test is timeout.", waiter.await(20, TimeUnit.SECONDS));
		assertEquals(3, actuals.size());
		for (String s : testdatas) {
			assertTrue(s, actuals.contains(s));
		}
		assertTrue("begin", beginEnd[0]);
		assertTrue("end", beginEnd[1]);
	}

	@Test
	public void testPutWithOpt() throws Exception {
		final Location location = new Location("testPutWithOpt", "testKey");
		final String testdata = new SimpleDateFormat().format(new Date())
				+ "\n";
		try {
			testPutWithOpt(location, testdata);
		} finally {
			testDelete(location);
		}
	}

	public void testPutWithOpt(Location location, final String testdata)
			throws Exception {
		final boolean[] beginEnd = new boolean[2];

		DefaultRiakObject ro = new DefaultRiakObject(location);
		ro.setContent(testdata.getBytes());

		RiakFuture waiter = this.target.put(ro, new DefaultPutOptions() {
			@Override
			public boolean getReturnBody() {
				return true;
			}
		}, new SiblingHandler() {

			@Override
			public void onError(RiakResponse response) throws Exception {
				fail(response.getMessage());
			}

			@Override
			public void begin() {
				beginEnd[0] = true;
			}

			@Override
			public void handle(RiakContentsResponse<RiakObject<byte[]>> response)
					throws Exception {
				RiakObject<byte[]> ro = response.getContents();
				assertEquals(testdata, new String(ro.getContent()));
			}

			@Override
			public void end() {
				beginEnd[1] = true;
			}
		});

		waitFor(waiter);
		assertTrue("begin", beginEnd[0]);
		assertTrue("end", beginEnd[1]);
	}

	@Test
	public void testDeleteWithQuorum() throws Exception {
		Location location = new Location("testDeleteWithQuorum", "delkey");
		testPut(location, "aaa");
		testDeleteWithQuorum(location);
	}

	public void testDeleteWithQuorum(Location location) throws Exception {
		final boolean[] is = { false };

		RiakFuture waiter = this.target.delete(location, Quorum.of(2),
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

	@Test
	public void testMove() throws Exception {
		final String from = "testMoveFrom";
		final String to = "testMoveTo";

		delete(from);
		delete(to);

		final int recordCount = 100;
		for (int i = 0; i < recordCount; i++) {
			testPut(new Location(from, String.valueOf(i)), "data" + i);
		}

		final CountDownLatch latch = new CountDownLatch(recordCount);
		this.target.listKeys(from, new TestingHandler<KeyResponse>() {
			@Override
			public void handle(RiakContentsResponse<KeyResponse> response)
					throws Exception {
				KeyResponse kr = response.getContents();
				if (kr.getDone()) {
					return;
				}
				for (final String k : kr.getKeys()) {
					RiakOperationsTest.this.target.delete(
							new Location(from, k), new TestingHandler<_>() {
								@Override
								public void handle(
										RiakContentsResponse<_> response)
										throws Exception {
									DefaultRiakObject ro = new DefaultRiakObject(
											new Location(to, k));
									ro.setContent("data".getBytes());
									RiakOperationsTest.this.target.put(ro,
											new TestingHandler<_>() {
												@Override
												public void handle(
														RiakContentsResponse<_> response)
														throws Exception {
													latch.countDown();
												}
											});
								}
							});

				}
			}
		});
		assertTrue("timeout.", latch.await(20, TimeUnit.SECONDS));
	}

	protected void delete(String bucket) throws Exception {
		final List<String> keys = new ArrayList<String>();
		RiakFuture rf = this.target.listKeys(bucket,
				new TestingHandler<KeyResponse>() {
					@Override
					public void handle(
							RiakContentsResponse<KeyResponse> response)
							throws Exception {
						keys.addAll(response.getContents().getKeys());
					}
				});
		waitFor(rf);
		final CountDownLatch latch = new CountDownLatch(keys.size());
		for (String s : keys) {
			this.target.delete(new Location(bucket, s),
					new TestingHandler<_>() {
						@Override
						public void handle(RiakContentsResponse<_> response)
								throws Exception {
							latch.countDown();
						}
					});
		}
		assertTrue("timeout.", latch.await(5, TimeUnit.SECONDS));
	}

	@Test
	public void testMapReduce() throws Exception {
		String bucket = "testMapReduce";
		try {
			for (int i = 0; i < 20; i++) {
				Location l = new Location(bucket, String.valueOf(i));
				int val = i + 10;
				testPut(l, String.valueOf(val));
			}

			testMapReduce(bucket);
		} finally {
			for (int i = 0; i < 20; i++) {
				Location l = new Location(bucket, String.valueOf(i));
				testDelete(l);
			}
		}
	}

	protected void testMapReduce(final String bucket) throws Exception {
		final boolean[] is = { false };

		final int[] actual = new int[1];
		RiakFuture waiter = this.target.mapReduce(
				new MapReduceQueryConstructor() {
					@Override
					public void cunstruct(MapReduceQuery query) {
						query.setInputs(MapReduceInputs.keyFilter(bucket,
								MapReduceKeyFilters.Transform.stringToInt(),
								MapReduceKeyFilters.Predicates.lessThanEq(10)));
						query.setQueries(
								NamedFunctionPhase.map(Erlang.map_object_value,
										false),
								NamedFunctionPhase.reduce(
										Erlang.reduce_string_to_integer, false),
								NamedFunctionPhase.reduce(Erlang.reduce_sum,
										true));
					}
				}, new TestingHandler<MapReduceResponse>() {
					@Override
					public void handle(
							RiakContentsResponse<MapReduceResponse> response)
							throws RiakException {
						if (response.getContents().getDone()) {
							is[0] = true;
						} else {
							ArrayNode an = response.getContents().getResponse();
							JsonNode jn = an.get(0);
							actual[0] = jn.getIntValue();
						}
					}
				});

		waitFor(waiter);
		assertEquals(165, actual[0]);
		assertTrue(is[0]);

	}

	@Test
	public void testMapReduceByRawJson() throws Exception {
		String bucket = "testMapReduceByRawJson";
		for (int i = 0; i < 20; i++) {
			Location l = new Location(bucket, String.valueOf(i));
			int val = i + 10;
			testPut(l, String.valueOf(val));
		}
		try {
			testMapReduceByRawJson(JsonUtil.getJsonPath(
					RiakOperationsTest.class, "testMapReduceByRawJson"));
		} finally {
			for (int i = 0; i < 20; i++) {
				Location l = new Location(bucket, String.valueOf(i));
				testDelete(l);
			}
		}
	}

	public void testMapReduceByRawJson(final String path) throws Exception {
		final boolean[] is = { false };

		final int[] actual = new int[1];
		RiakFuture waiter = this.target.mapReduce(loadJson(path),
				new TestingHandler<MapReduceResponse>() {
					@Override
					public void handle(
							RiakContentsResponse<MapReduceResponse> response)
							throws RiakException {
						if (response.getContents().getDone()) {
							is[0] = true;
						} else {
							ArrayNode an = response.getContents().getResponse();
							JsonNode jn = an.get(0);
							actual[0] = jn.getIntValue();
						}
					}
				});

		waitFor(waiter);
		assertEquals(165, actual[0]);
		assertTrue(is[0]);

	}

	String loadJson(final String path) {
		final String[] json = new String[1];
		new Streams.using<InputStream, IOException>() {
			@Override
			public InputStream open() throws IOException {
				ClassLoader cl = ProtoBufRiakOperationsTest.class
						.getClassLoader();
				return cl.getResourceAsStream(path);
			}

			@Override
			public void handle(InputStream stream) throws IOException {
				json[0] = Streams.readText(stream);
			}

			@Override
			public void happen(IOException exception) {
				throw new Streams.IORuntimeException(exception);
			}

		};
		return json[0];
	}

	@Test
	public void testMapReduceContainsJson() throws Exception {
		final String bucket = "testMapReduceContainsJson";
		String key = "left::right";
		Location location = new Location(bucket, key);

		String data = "{\"a\":\"xxxx\",\"b\":\"c\"}";
		testPut(location, data);

		final JsonNode[] actual = new JsonNode[1];
		RiakFuture waiter = this.target.mapReduce(
				new MapReduceQueryConstructor() {

					@Override
					public void cunstruct(MapReduceQuery query) {
						query.setInputs(MapReduceInputs
								.keyFilter(bucket,
										MapReduceKeyFilters.Transform.tokenize(
												"::", 1),
										MapReduceKeyFilters.Predicates
												.equal("left")));
						query.setQueries(JavaScriptPhase.BuiltIns.map(
								JavaScript.mapValuesJson, true));
					}
				}, new TestingHandler<MapReduceResponse>() {
					@Override
					public void handle(
							RiakContentsResponse<MapReduceResponse> response)
							throws Exception {
						MapReduceResponse mrr = response.getContents();
						if (mrr.getDone()) {
						} else {
							ArrayNode an = mrr.getResponse();
							actual[0] = an.get(0);
						}
					}
				});

		waitFor(waiter);
		assertNotNull(actual[0]);
		assertEquals(actual[0].getClass(), ObjectNode.class);

		testDelete(location);
	}

	@Test
	public void testFromOfficialsByRawJson() throws Exception {
		final String bucket = "alice";
		String p1 = "Alice was beginning to get very tired of sitting by her sister on the"
				+ " bank, and of having nothing to do: once or twice she had peeped into the"
				+ " book her sister was reading, but it had no pictures or conversations in"
				+ " it, 'and what is the use of a book,' thought Alice 'without pictures or"
				+ " conversation?'";
		testPut(new Location(bucket, "p1"), p1);

		String p2 = "So she was considering in her own mind (as well as she could, for the"
				+ " hot day made her feel very sleepy and stupid), whether the pleasure"
				+ " of making a daisy-chain would be worth the trouble of getting up and"
				+ " picking the daisies, when suddenly a White Rabbit with pink eyes ran"
				+ " close by her.";
		testPut(new Location(bucket, "p2"), p2);

		String p5 = "The rabbit-hole went straight on like a tunnel for some way, and then"
				+ " dipped suddenly down, so suddenly that Alice had not a moment to think"
				+ " about stopping herself before she found herself falling down a very deep"
				+ " well.";
		testPut(new Location(bucket, "p5"), p5);

		final boolean[] is = { false };

		String query = loadJson(JsonUtil.getJsonPath(RiakOperationsTest.class,
				"testFromOfficialsByRawJson"));

		final ArrayNode[] actual = new ArrayNode[1];
		RiakFuture waiter = this.target.mapReduce(query,
				new TestingHandler<MapReduceResponse>() {
					@Override
					public void handle(
							RiakContentsResponse<MapReduceResponse> response)
							throws Exception {
						MapReduceResponse mrr = response.getContents();
						if (mrr.getDone()) {
							is[0] = true;
						} else {
							actual[0] = mrr.getResponse();
						}
					}
				});

		waitFor(waiter);

		testDelete(new Location(bucket, "p1"));
		testDelete(new Location(bucket, "p2"));
		testDelete(new Location(bucket, "p5"));

		JsonNode expected = JsonUtil.read(RiakOperationsTest.class,
				"testFromOfficialsByRawJson_Expected");
		assertEquals(expected, actual[0]);
	}

	protected void waitFor(final RiakFuture waiter) throws InterruptedException {
		assertTrue("test is timeout.", waiter.await(3, TimeUnit.SECONDS));
		assertTrue(waiter.isDone());
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

		DefaultRiakObject ro = new DefaultRiakObject(location);
		ro.setContent(testdata.getBytes());
		final Location[] loc = new Location[1];
		RiakFuture waiter = this.target.post(ro,
				new TestingHandler<RiakObject<byte[]>>() {
					@Override
					public void handle(
							RiakContentsResponse<RiakObject<byte[]>> response)
							throws Exception {
						RiakObject<byte[]> returned = response.getContents();
						assertNotNull(returned.getLocation());
						Location l = returned.getLocation();
						assertEquals(location.getBucket(), l.getBucket());
						assertFalse(l.getKey().isEmpty());
						loc[0] = l;
					}
				});
		assertTrue("time is over.", waiter.await(5, TimeUnit.SECONDS));

		// wait(waiter, is);
		return loc[0];
	}

	public Location testPostWithReturn(final Location location,
			final String testdata) throws Exception {
		DefaultRiakObject ro = new DefaultRiakObject(location);
		ro.setContent(testdata.getBytes());

		PostOptions options = new DefaultPostOptions() {
			@Override
			public boolean getReturnBody() {
				return true;
			}
		};

		final Location[] loc = new Location[1];
		RiakFuture waiter = this.target.post(ro, options,
				new TestingHandler<RiakObject<byte[]>>() {
					@Override
					public void handle(
							RiakContentsResponse<RiakObject<byte[]>> response)
							throws Exception {
						RiakObject<byte[]> returned = response.getContents();
						assertNotNull(returned.getLocation());
						Location l = returned.getLocation();
						assertEquals(location.getBucket(), l.getBucket());
						assertFalse(l.getKey().isEmpty());
						loc[0] = l;
					}
				});

		waitFor(waiter);
		return loc[0];
	}

	@Test
	public void testExceptionHandling() throws Exception {
		final String expected = "testMessage";
		final String[] actual = new String[1];
		final Exception exception = new Exception(expected);
		RiakFuture rf = this.target.ping(new RiakResponseHandler<String>() {
			@Override
			public void onError(RiakResponse response) throws Exception {
				actual[0] = response.getMessage();
			}

			@Override
			public void handle(RiakContentsResponse<String> response)
					throws Exception {
				throw exception;
			}
		});
		waitFor(rf);
		assertFalse(rf.isSuccess());
		assertEquals(exception, rf.getCause());
		assertEquals(expected, actual[0]);
	}

	@Test
	public void testErrorHandling() throws Exception {
		final String expected = "testMessage";
		final String[] actual = new String[1];
		final Error error = new Error(expected);
		RiakFuture rf = this.target.ping(new RiakResponseHandler<String>() {
			@Override
			public void onError(RiakResponse response) throws Exception {
				actual[0] = response.getMessage();
			}

			@Override
			public void handle(RiakContentsResponse<String> response)
					throws Exception {
				throw error;
			}
		});
		waitFor(rf);
		assertFalse(rf.isSuccess());
		assertEquals(error, rf.getCause());
		assertEquals(expected, actual[0]);
	}
}
