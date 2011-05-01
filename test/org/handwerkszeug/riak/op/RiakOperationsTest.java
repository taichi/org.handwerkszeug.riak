package org.handwerkszeug.riak.op;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.mapreduce.MapReduceInputs;
import org.handwerkszeug.riak.mapreduce.MapReduceKeyFilters;
import org.handwerkszeug.riak.mapreduce.MapReduceQuery;
import org.handwerkszeug.riak.mapreduce.MapReduceQueryConstructor;
import org.handwerkszeug.riak.mapreduce.MapReduceResponse;
import org.handwerkszeug.riak.mapreduce.NamedFunctionPhase;
import org.handwerkszeug.riak.model.Bucket;
import org.handwerkszeug.riak.model.DefaultGetOptions;
import org.handwerkszeug.riak.model.DefaultPutOptions;
import org.handwerkszeug.riak.model.DefaultRiakObject;
import org.handwerkszeug.riak.model.Erlang;
import org.handwerkszeug.riak.model.KeyResponse;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.Quorum;
import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.model.RiakResponse;
import org.handwerkszeug.riak.pbc.PbcRiakOperationsTest;
import org.handwerkszeug.riak.util.JsonUtil;
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
		final AtomicBoolean waiter = new AtomicBoolean(false);

		final boolean[] is = { false };
		this.target.ping(new RiakResponseHandler<String>() {
			@Override
			public void onError(RiakResponse response) throws RiakException {
				waiter.compareAndSet(false, true);
			}

			@Override
			public void handle(RiakContentsResponse<String> response)
					throws RiakException {
				try {
					assertEquals("pong", response.getContents());
					is[0] = true;
				} finally {
					waiter.compareAndSet(false, true);
				}
			}
		});

		wait(waiter, is);
	}

	@Test
	public void testListBuckets() throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		target.listBuckets(new RiakResponseHandler<List<String>>() {
			@Override
			public void onError(RiakResponse response) throws RiakException {
				waiter.compareAndSet(false, true);
			}

			@Override
			public void handle(RiakContentsResponse<List<String>> response)
					throws RiakException {
				try {
					List<String> keys = response.getContents();
					assertNotNull(keys);
					assertTrue(0 < keys.size());

					is[0] = true;
				} finally {
					waiter.compareAndSet(false, true);
				}

			}
		});

		wait(waiter, is);
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

	public void testListKeys(String bucket, int allsize) throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		final int[] counter = { 0 };
		target.listKeys(bucket, new RiakResponseHandler<KeyResponse>() {

			@Override
			public void onError(RiakResponse response) throws RiakException {
				waiter.compareAndSet(false, true);
			}

			@Override
			public void handle(RiakContentsResponse<KeyResponse> response)
					throws RiakException {
				KeyResponse kr = response.getContents();
				List<String> list = kr.getKeys();
				counter[0] += list.size();

				if (kr.getDone()) {
					waiter.compareAndSet(false, true);
					is[0] = true;
				}

			}
		});

		wait(waiter, is);
		assertEquals(allsize, counter[0]);
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
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };
		bucket.setAllowMulti(true);
		bucket.setNumberOfReplicas(1);

		target.setBucket(bucket, new RiakResponseHandler<_>() {
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

	public Bucket testBucketGet(String bucket) throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };
		final Bucket[] bu = new Bucket[1];

		target.getBucket(bucket, new RiakResponseHandler<Bucket>() {
			@Override
			public void onError(RiakResponse response) throws RiakException {
				waiter.compareAndSet(false, true);
			}

			@Override
			public void handle(RiakContentsResponse<Bucket> response)
					throws RiakException {
				try {
					assertNotNull(response.getContents());
					bu[0] = response.getContents();
					is[0] = true;
				} finally {
					waiter.compareAndSet(false, true);
				}
			}
		});

		wait(waiter, is);
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
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		this.target.get(location,
				new RiakResponseHandler<RiakObject<byte[]>>() {
					@Override
					public void onError(RiakResponse response)
							throws RiakException {
						waiter.compareAndSet(false, true);
					}

					@Override
					public void handle(
							RiakContentsResponse<RiakObject<byte[]>> response)
							throws RiakException {
						try {
							RiakObject<byte[]> content = response.getContents();
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
					public void onError(RiakResponse response)
							throws RiakException {
						waiter.compareAndSet(false, true);
					}

					@Override
					public void handle(
							RiakContentsResponse<List<RiakObject<byte[]>>> response)
							throws RiakException {
						try {
							assertEquals(0, response.getContents().size());
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

	@Test
	public void testGetWithOpt() throws Exception {
		final Location location = new Location("testGetWithOpt", "testKey");
		final String testdata = new SimpleDateFormat().format(new Date())
				+ "\n";
		testPut(location, testdata);
		try {
			testGetWithOpt(location, testdata);
		} finally {
			testDelete(location);
		}
	}

	protected void testGetWithOpt(final Location location, final String testdata)
			throws InterruptedException {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		target.get(location, new DefaultGetOptions() {
			@Override
			public Quorum getReadQuorum() {
				return Quorum.of(2);
			}
		}, new RiakResponseHandler<RiakObject<byte[]>>() {
			@Override
			public void onError(RiakResponse response) throws RiakException {
				waiter.compareAndSet(false, true);
			}

			@Override
			public void handle(RiakContentsResponse<RiakObject<byte[]>> response)
					throws RiakException {
				try {
					RiakObject<byte[]> ro = response.getContents();
					assertEquals(location, ro.getLocation());
					assertEquals(testdata, new String(ro.getContent()));
					is[0] = true;
				} finally {
					waiter.compareAndSet(false, true);
				}
			}
		});

		wait(waiter, is);
	}

	@Test
	public void testGetWithSibling() throws Exception {
		final Location location = new Location("testGetWithSibling", "testKey");
		final String testdata = new SimpleDateFormat().format(new Date())
				+ "\n";
		testPut(location, testdata);
		try {
			testGetWithSibling(location, testdata);
		} finally {
			testDelete(location);
		}
	}

	@Test
	public void testGetNoContents() throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };
		Location location = new Location("testGetNoContents", "nocont");

		target.get(location, new RiakResponseHandler<RiakObject<byte[]>>() {
			@Override
			public void onError(RiakResponse response) throws RiakException {
				waiter.compareAndSet(false, true);
			}

			@Override
			public void handle(RiakContentsResponse<RiakObject<byte[]>> response)
					throws RiakException {
				try {
					assertNull(response.getContents());
					assertNotNull(response.getMessage());
					assertFalse(response.getMessage().isEmpty());
					is[0] = true;
				} finally {
					waiter.compareAndSet(false, true);
				}
			}
		});

		wait(waiter, is);
	}

	protected void testGetWithSibling(final Location location,
			final String testdata) throws InterruptedException {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		target.get(location, new DefaultGetOptions() {
			@Override
			public Quorum getReadQuorum() {
				return Quorum.of(2);
			}
		}, new SiblingHandler() {
			@Override
			public void begin() {
				assertTrue(true);
			}

			@Override
			public void onError(RiakResponse response) throws RiakException {
				waiter.compareAndSet(false, true);
			}

			@Override
			public void handle(RiakContentsResponse<RiakObject<byte[]>> response)
					throws RiakException {
				RiakObject<byte[]> ro = response.getContents();
				assertEquals(location, ro.getLocation());
				assertEquals(testdata, new String(ro.getContent()));
				is[0] = true;
			}

			@Override
			public void end() {
				waiter.compareAndSet(false, true);
			}
		});
		wait(waiter, is);
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
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		RiakObject<byte[]> ro = new DefaultRiakObject(location) {
			@Override
			public byte[] getContent() {
				return testdata.getBytes();
			}
		};

		target.put(ro, new DefaultPutOptions() {
			@Override
			public boolean getReturnBody() {
				return true;
			}
		}, new RiakResponseHandler<List<RiakObject<byte[]>>>() {
			@Override
			public void onError(RiakResponse response) throws RiakException {
				waiter.compareAndSet(false, true);
			}

			@Override
			public void handle(
					RiakContentsResponse<List<RiakObject<byte[]>>> response)
					throws RiakException {
				try {
					assertEquals(1, response.getContents().size());
					RiakObject<byte[]> res = response.getContents().get(0);
					assertEquals(testdata, new String(res.getContent()));
					is[0] = true;
				} finally {
					waiter.compareAndSet(false, true);
				}
			}
		});

		wait(waiter, is);
	}

	@Test
	public void testDeleteWithQuorum() throws Exception {
		Location location = new Location("testDeleteWithQuorum", "delkey");
		testPut(location, "aaa");
		testDeleteWithQuorum(location);
	}

	public void testDeleteWithQuorum(Location location) throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		target.delete(location, Quorum.of(2), new RiakResponseHandler<_>() {
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

	public void testMapReduce(final String bucket) throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		final int[] actual = new int[1];
		target.mapReduce(new MapReduceQueryConstructor() {
			@Override
			public void cunstruct(MapReduceQuery query) {
				query.setInputs(MapReduceInputs.keyFilter(bucket,
						MapReduceKeyFilters.Transform.stringToInt(),
						MapReduceKeyFilters.Predicates.lessThanEq(10)));
				query.setQueries(NamedFunctionPhase.map(
						Erlang.map_object_value, false), NamedFunctionPhase
						.reduce(Erlang.reduce_string_to_integer, false),
						NamedFunctionPhase.reduce(Erlang.reduce_sum, true));
			}
		}, new RiakResponseHandler<MapReduceResponse>() {
			@Override
			public void onError(RiakResponse response) throws RiakException {
				waiter.compareAndSet(false, true);
			}

			@Override
			public void handle(RiakContentsResponse<MapReduceResponse> response)
					throws RiakException {
				if (response.getContents().getDone()) {
					waiter.compareAndSet(false, true);
					is[0] = true;
				} else {
					ArrayNode an = (ArrayNode) response.getContents()
							.getResponse();
					JsonNode jn = an.get(0);
					actual[0] = jn.getIntValue();
				}
			}
		});

		wait(waiter, is);
		assertEquals(165, actual[0]);
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
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		final int[] actual = new int[1];
		target.mapReduce(loadJson(path),
				new RiakResponseHandler<MapReduceResponse>() {
					@Override
					public void onError(RiakResponse response)
							throws RiakException {
						waiter.compareAndSet(false, true);
					}

					@Override
					public void handle(
							RiakContentsResponse<MapReduceResponse> response)
							throws RiakException {
						if (response.getContents().getDone()) {
							waiter.compareAndSet(false, true);
							is[0] = true;
						} else {
							ArrayNode an = (ArrayNode) response.getContents()
									.getResponse();
							JsonNode jn = an.get(0);
							actual[0] = jn.getIntValue();
						}
					}
				});

		wait(waiter, is);
		assertEquals(165, actual[0]);
	}

	String loadJson(final String path) {
		final String[] json = new String[1];
		new Streams.using<InputStream, IOException>() {
			@Override
			public InputStream open() throws IOException {
				ClassLoader cl = PbcRiakOperationsTest.class.getClassLoader();
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

	protected void wait(final AtomicBoolean waiter, final boolean[] is)
			throws InterruptedException {
		while (waiter.get() == false) {
			Thread.sleep(10);
		}
		assertTrue(is[0]);
	}
}
