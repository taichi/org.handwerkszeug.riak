package org.handwerkszeug.riak.pbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.handwerkszeug.riak.Hosts;
import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.mapreduce.MapReduceQuery;
import org.handwerkszeug.riak.mapreduce.MapReduceQueryConstructor;
import org.handwerkszeug.riak.mapreduce.MapReduceResponse;
import org.handwerkszeug.riak.mapreduce.NamedFunctionPhase;
import org.handwerkszeug.riak.model.Bucket;
import org.handwerkszeug.riak.model.DefaultGetOptions;
import org.handwerkszeug.riak.model.DefaultPutOptions;
import org.handwerkszeug.riak.model.DefaultRiakObject;
import org.handwerkszeug.riak.model.Erlang;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.Quorum;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.model.RiakResponse;
import org.handwerkszeug.riak.model.ServerInfo;
import org.handwerkszeug.riak.op.KeyHandler;
import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.handwerkszeug.riak.op.SiblingHandler;
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
	public void testListBuckets() throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		target.listBuckets(new RiakResponseHandler<List<String>>() {
			@Override
			public void handle(RiakResponse<List<String>> response)
					throws RiakException {
				try {
					List<String> keys = response.getResponse();
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
		for (int i = 0; i < 20; i++) {
			Location l = new Location(bucket, String.valueOf(i));
			testPut(l, testdata);
		}

		testListKeys(bucket, 20);

		for (int i = 0; i < 20; i++) {
			Location l = new Location(bucket, String.valueOf(i));
			testDelete(l);
		}
	}

	public void testListKeys(String bucket, int allsize) throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		final int[] counter = { 0 };
		target.listKeys(bucket, new KeyHandler() {

			@Override
			public void handle(RiakResponse<_> response) throws RiakException {
				if (response.isErrorResponse()) {
					waiter.compareAndSet(false, true);
				}
			}

			@Override
			public void handleKeys(RiakResponse<List<String>> response,
					boolean done) {
				List<String> list = response.getResponse();
				counter[0] += list.size();

				if (done) {
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
			public void handle(RiakResponse<_> response) throws RiakException {
				try {
					assertFalse(response.isErrorResponse());
					is[0] = true;
				} finally {
					waiter.compareAndSet(false, true);
				}
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
			public void handle(RiakResponse<Bucket> response)
					throws RiakException {
				try {
					assertFalse(response.isErrorResponse());
					assertNotNull(response.getResponse());
					bu[0] = response.getResponse();
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
					public void handle(RiakResponse<RiakObject<byte[]>> response)
							throws RiakException {
						try {
							assertFalse(response.isErrorResponse());
							RiakObject<byte[]> content = response.getResponse();
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
					public void handle(
							RiakResponse<List<RiakObject<byte[]>>> response)
							throws RiakException {
						try {
							assertFalse(response.isErrorResponse());
							assertEquals(0, response.getResponse().size());
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
			public void handle(RiakResponse<_> response) throws RiakException {
				try {
					assertFalse(response.isErrorResponse());
					is[0] = true;
				} finally {
					waiter.compareAndSet(false, true);
				}
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
			public void handle(RiakResponse<RiakObject<byte[]>> response)
					throws RiakException {
				try {
					assertFalse(response.isErrorResponse());
					RiakObject<byte[]> ro = response.getResponse();
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
			public void handle(RiakResponse<RiakObject<byte[]>> response)
					throws RiakException {
				assertFalse(response.isErrorResponse());
				RiakObject<byte[]> ro = response.getResponse();
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

		RiakObject<byte[]> ro = new DefaultRiakObject(location) {
			@Override
			public byte[] getContent() {
				return testdata.getBytes();
			}
		};
		final boolean[] is = { false };

		target.put(ro, new DefaultPutOptions() {
			@Override
			public boolean getReturnBody() {
				return true;
			}
		}, new RiakResponseHandler<List<RiakObject<byte[]>>>() {
			@Override
			public void handle(RiakResponse<List<RiakObject<byte[]>>> response)
					throws RiakException {
				try {
					assertFalse(response.isErrorResponse());
					assertEquals(1, response.getResponse().size());
					RiakObject<byte[]> res = response.getResponse().get(0);
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
			public void handle(RiakResponse<_> response) throws RiakException {
				try {
					assertFalse(response.isErrorResponse());
					is[0] = true;
				} finally {
					waiter.compareAndSet(false, true);
				}
			}
		});

		wait(waiter, is);
	}

	@Test
	public void testMapReduce() throws Exception {
		String bucket = "testMapReduce";
		List<Integer> exp = new ArrayList<Integer>();
		for (int i = 0; i < 20; i++) {
			Location l = new Location(bucket, String.valueOf(i));
			int val = i + 10;
			testPut(l, String.valueOf(val));
			exp.add(val);
		}

		testMapReduce(bucket, 20, exp);

		for (int i = 0; i < 20; i++) {
			Location l = new Location(bucket, String.valueOf(i));
			testDelete(l);
		}
	}

	public void testMapReduce(final String bucket, int keys, List<Integer> exp)
			throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		final List<Integer> actual = new ArrayList<Integer>();
		target.mapReduce(new MapReduceQueryConstructor() {
			@Override
			public void cunstruct(MapReduceQuery query) {
				query.setInputs(bucket);
				query.setQueries(NamedFunctionPhase
						.map(Erlang.map_object_value));
			}
		}, new RiakResponseHandler<MapReduceResponse>() {
			@Override
			public void handle(RiakResponse<MapReduceResponse> response)
					throws RiakException {
				if (response.isErrorResponse()) {
					waiter.compareAndSet(false, true);
				} else {
					if (response.getResponse().getDone()) {
						waiter.compareAndSet(false, true);
						is[0] = true;
					} else {
						ArrayNode an = (ArrayNode) response.getResponse()
								.getResponse();
						JsonNode jn = an.get(0);
						actual.add(jn.getValueAsInt());
					}
				}
			}
		});

		wait(waiter, is);
		assertEquals(20, actual.size());
		Collections.sort(actual);
		assertEquals(exp, actual);
	}

	@Test
	public void testClientId() throws Exception {
		String id = "AXZH";
		testSetClientId(id);
		testGetClientId(id);

		try {
			target.setClientId("12345", new RiakResponseHandler<_>() {
				@Override
				public void handle(RiakResponse<_> response)
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
			public void handle(RiakResponse<_> response) throws RiakException {
				try {
					assertFalse(response.isErrorResponse());
					is[0] = true;
				} finally {
					waiter.compareAndSet(false, true);
				}
			}
		});

		wait(waiter, is);
	}

	public void testGetClientId(final String id) throws Exception {
		final AtomicBoolean waiter = new AtomicBoolean(false);
		final boolean[] is = { false };

		this.target.getClientId(new RiakResponseHandler<String>() {
			@Override
			public void handle(RiakResponse<String> response)
					throws RiakException {
				try {
					assertEquals(id, response.getResponse());
					is[0] = true;
				} finally {
					waiter.compareAndSet(false, true);
				}
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
			public void handle(RiakResponse<ServerInfo> response)
					throws RiakException {
				try {
					ServerInfo info = response.getResponse();
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

	protected void wait(final AtomicBoolean waiter, final boolean[] is)
			throws InterruptedException {
		while (waiter.get() == false) {
			Thread.sleep(10);
		}
		assertTrue(is[0]);
	}
}
