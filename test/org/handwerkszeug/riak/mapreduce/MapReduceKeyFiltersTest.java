package org.handwerkszeug.riak.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.handwerkszeug.riak.Hosts;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.mapreduce.MapReduceKeyFilters.Predicates;
import org.handwerkszeug.riak.mapreduce.MapReduceKeyFilters.Transform;
import org.handwerkszeug.riak.model.DefaultRiakObject;
import org.handwerkszeug.riak.model.JavaScript;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.model.RiakFuture;
import org.handwerkszeug.riak.op.TestingHandler;
import org.handwerkszeug.riak.transport.protobuf.ProtoBufRiakConfig;
import org.handwerkszeug.riak.transport.protobuf.ProtoBufRiakOperations;
import org.handwerkszeug.riak.transport.protobuf.internal.ProtoBufPipelineFactory;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MapReduceKeyFiltersTest {

	static final String TEST_BUCKET = "MapReduceKeyFiltersTest";

	ClientBootstrap bootstrap;
	Channel channel;

	static final ProtoBufRiakConfig config = ProtoBufRiakConfig.newConfig(
			Hosts.RIAK_HOST, Hosts.RIAK_PB_PORT);

	ProtoBufRiakOperations target;

	@Before
	public void setUp() throws Exception {
		this.bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));

		this.bootstrap.setPipelineFactory(new ProtoBufPipelineFactory());

		ChannelFuture future = this.bootstrap.connect(config.getRiakAddress());
		this.channel = future.awaitUninterruptibly().getChannel();
		this.target = new ProtoBufRiakOperations(this.channel);
	}

	@After
	public void tearDown() throws Exception {
		this.channel.close().awaitUninterruptibly();
		this.bootstrap.releaseExternalResources();
	}

	void waitFor(RiakFuture rf) throws Exception {
		assertTrue("timeout.", rf.await(5, TimeUnit.SECONDS));
	}

	void put(String key) throws Exception {
		Location location = new Location(TEST_BUCKET, key);
		DefaultRiakObject ro = new DefaultRiakObject(location);
		String s = key + " [" + new Date() + "]";
		ro.setContent(s.getBytes());
		ro.setContentEncoding("UTF-8");

		System.out.println("**** " + ro);

		RiakFuture rf = this.target.put(ro, new TestingHandler<_>() {
			@Override
			public void handle(RiakContentsResponse<_> response)
					throws Exception {
				assertTrue(true);

			}
		});
		waitFor(rf);
	}

	void delete(String key) throws Exception {
		RiakFuture rf = this.target.delete(new Location(TEST_BUCKET, key),
				new TestingHandler<_>() {
					@Override
					public void handle(RiakContentsResponse<_> response)
							throws Exception {
						assertTrue(true);
					}
				});
		waitFor(rf);
	}

	void assertFilters(final Iterable<MapReduceKeyFilter> keyFilters,
			int expectedCount) throws Exception {
		final List<String> list = new ArrayList<String>();
		RiakFuture rf = this.target.mapReduce(new MapReduceQueryConstructor() {
			@Override
			public void cunstruct(MapReduceQuery query) {
				query.setInputs(MapReduceInputs.keyFilter(TEST_BUCKET,
						keyFilters));
				query.setQueries(JavaScriptPhase.BuiltIns.map(
						JavaScript.mapValues, true));
			}
		}, new TestingHandler<MapReduceResponse>() {
			@Override
			public void handle(RiakContentsResponse<MapReduceResponse> response)
					throws Exception {
				MapReduceResponse mrr = response.getContents();
				if (mrr.getDone() == false) {
					ArrayNode an = mrr.getResponse();
					System.out.println(an);
					for (JsonNode jn : an) {
						list.add(jn.getTextValue());
					}
				}
			}
		});

		waitFor(rf);
		System.out.println(list);
		assertEquals(expectedCount, list.size());
	}

	@Test
	public void testTransformInt() throws Exception {
		try {
			for (int i = 0; i < 5; i++) {
				put(String.valueOf(i));
			}

			List<MapReduceKeyFilter> list = Arrays.asList(
					Transform.stringToInt(), Transform.intToString(),
					Transform.stringToInt(), Predicates.greaterThan(1));
			assertFilters(list, 3);

			list = Arrays.asList(Transform.stringToInt(),
					Predicates.greaterThan(1L));
			assertFilters(list, 3);

			list = Arrays.asList(Transform.stringToInt(),
					Predicates.lessThan(3));
			assertFilters(list, 3);

			list = Arrays.asList(Transform.stringToInt(),
					Predicates.lessThan(3L));
			assertFilters(list, 3);

			list = Arrays.asList(Transform.stringToInt(),
					Predicates.greaterThanEq(2));
			assertFilters(list, 3);

			list = Arrays.asList(Transform.stringToInt(),
					Predicates.greaterThanEq(2L));
			assertFilters(list, 3);

			list = Arrays.asList(Transform.stringToInt(),
					Predicates.lessThanEq(2));
			assertFilters(list, 3);

			list = Arrays.asList(Transform.stringToInt(),
					Predicates.lessThanEq(2L));
			assertFilters(list, 3);

			list = Arrays.asList(Transform.stringToInt(),
					Predicates.notEqual(3));
			assertFilters(list, 4);

			list = Arrays.asList(Transform.stringToInt(),
					Predicates.notEqual(3L));
			assertFilters(list, 4);

			list = Arrays.asList(Transform.stringToInt(), Predicates.equal(2));
			assertFilters(list, 1);

			list = Arrays.asList(Transform.stringToInt(), Predicates.equal(2L));
			assertFilters(list, 1);

			list = Arrays.asList(Transform.stringToInt(),
					Predicates.between(1, 3));
			assertFilters(list, 3);

			list = Arrays.asList(Transform.stringToInt(),
					Predicates.between(1, 3, false));
			assertFilters(list, 1);

			list = Arrays.asList(Transform.stringToInt(),
					Predicates.between(1L, 3L));
			assertFilters(list, 3);

			list = Arrays.asList(Transform.stringToInt(),
					Predicates.between(1L, 3L, false));
			assertFilters(list, 1);

			list = Arrays.asList(Transform.stringToInt(),
					Predicates.intMember(1, 2, 3));
			assertFilters(list, 3);

			list = Arrays.asList(Transform.stringToInt(),
					Predicates.longMember(0, 4));
			assertFilters(list, 2);

		} finally {
			for (int i = 0; i < 5; i++) {
				delete(String.valueOf(i));
			}
		}
	}

	@Test
	public void testTransformFloat() throws Exception {
		try {
			for (int i = 0; i < 5; i++) {
				put(Float.toString(i));
			}

			List<MapReduceKeyFilter> list = Arrays.asList(
					Transform.stringToFloat(), Transform.floatToString(),
					Transform.stringToFloat(), Predicates.greaterThan(1.0f));
			assertFilters(list, 3);

			list = Arrays.asList(Transform.stringToFloat(),
					Predicates.greaterThan(1.0));
			assertFilters(list, 3);

			list = Arrays.asList(Transform.stringToFloat(),
					Predicates.lessThan(3.0));
			assertFilters(list, 3);

			list = Arrays.asList(Transform.stringToFloat(),
					Predicates.lessThan(3.0f));
			assertFilters(list, 3);

			list = Arrays.asList(Transform.stringToFloat(),
					Predicates.greaterThanEq(2.0));
			assertFilters(list, 3);

			list = Arrays.asList(Transform.stringToFloat(),
					Predicates.greaterThanEq(2.0f));
			assertFilters(list, 3);

			list = Arrays.asList(Transform.stringToFloat(),
					Predicates.lessThanEq(2.0));
			assertFilters(list, 3);

			list = Arrays.asList(Transform.stringToFloat(),
					Predicates.lessThanEq(2.0f));
			assertFilters(list, 3);

			list = Arrays.asList(Transform.stringToFloat(),
					Predicates.notEqual(3.0));
			assertFilters(list, 4);

			list = Arrays.asList(Transform.stringToFloat(),
					Predicates.notEqual(3.0f));
			assertFilters(list, 4);

			list = Arrays.asList(Transform.stringToFloat(),
					Predicates.equal(2.0));
			assertFilters(list, 1);

			list = Arrays.asList(Transform.stringToFloat(),
					Predicates.equal(2.0f));
			assertFilters(list, 1);

			list = Arrays.asList(Transform.stringToFloat(),
					Predicates.between(1.0, 3.0));
			assertFilters(list, 3);

			list = Arrays.asList(Transform.stringToFloat(),
					Predicates.between(1.0, 3.0, false));
			assertFilters(list, 1);

			list = Arrays.asList(Transform.stringToFloat(),
					Predicates.between(1.0f, 3.0f));
			assertFilters(list, 3);

			list = Arrays.asList(Transform.stringToFloat(),
					Predicates.between(1.0f, 3.0f, false));
			assertFilters(list, 1);

			list = Arrays.asList(Transform.stringToFloat(),
					Predicates.floatMember(1.0f, 2.0f));
			assertFilters(list, 2);

			list = Arrays.asList(Transform.stringToFloat(),
					Predicates.doubleMember(1.0, 3.0, 4.0));
			assertFilters(list, 3);
		} finally {
			for (int i = 0; i < 5; i++) {
				delete(Float.toString(i));
			}
		}
	}

	@Test
	public void testTransformString() throws Exception {
		String[] ary = { "a,0", "b,1", "c,2", "d,3", "e,4" };
		try {
			for (String s : ary) {
				put(s);
			}

			List<MapReduceKeyFilter> list = Arrays.asList(
					Transform.tokenize(",", 2), Transform.stringToInt(),
					Predicates.greaterThanEq(2));
			assertFilters(list, 3);

			list = Arrays.asList(Transform.toLower(), Transform.toUpper(),
					Transform.tokenize(",", 1), Predicates.greaterThan("B"));
			assertFilters(list, 3);
		} finally {
			for (String s : ary) {
				delete(s);
			}
		}
	}

	@Test
	public void testStringSimilarity() throws Exception {
		String[] ary = { "1a2", "1b3", "2c4", "2d4", "3e5" };
		try {
			for (String s : ary) {
				put(s);
			}

			List<MapReduceKeyFilter> list = Arrays.asList(Predicates
					.greaterThan("2c"));
			assertFilters(list, 3);

			list = Arrays.asList(Predicates.lessThan("2c"));
			assertFilters(list, 2);

			list = Arrays.asList(Predicates.greaterThanEq("2d4"));
			assertFilters(list, 2);

			list = Arrays.asList(Predicates.lessThanEq("1b3"));
			assertFilters(list, 2);

			list = Arrays.asList(Predicates.notEqual("1b3"));
			assertFilters(list, 4);

			list = Arrays.asList(Predicates.equal("1b3"));
			assertFilters(list, 1);

			list = Arrays.asList(Predicates.between("1b3", "3e5"));
			assertFilters(list, 4);

			list = Arrays.asList(Predicates.between("1b3", "3e5", false));
			assertFilters(list, 2);

			list = Arrays.asList(Predicates.startsWith("1a"));
			assertFilters(list, 1);

			list = Arrays.asList(Predicates.endsWith("4"));
			assertFilters(list, 2);

			list = Arrays.asList(Predicates.similarTo("2a4", 1));
			assertFilters(list, 2);

			list = Arrays.asList(Predicates.setMember("1a2", "2c4", "3e5"));
			assertFilters(list, 3);

			list = Arrays.asList(Predicates.matches("[12][a-z]\\d+"));
			assertFilters(list, 4);

		} finally {
			for (String s : ary) {
				delete(s);
			}
		}
	}

	@Test
	public void testUrlDecode() throws Exception {
		String[] ary = { "1%2F2", "1%2F3", "2%2F4", "3%2F4", "3%2F5" };
		try {
			for (String s : ary) {
				put(s);
			}

			List<MapReduceKeyFilter> list = Arrays.asList(
					MapReduceKeyFilters.Transform.urldecode(),
					MapReduceKeyFilters.Transform.tokenize("/", 1),
					MapReduceKeyFilters.Transform.stringToInt(),
					MapReduceKeyFilters.Predicates.greaterThan(2));
			assertFilters(list, 2);

		} finally {
			for (String s : ary) {
				delete(s);
			}
		}
	}

	@Test
	public void testLogicalFilters() throws Exception {
		try {
			for (int i = 0; i < 5; i++) {
				put(String.valueOf(i));
			}

			List<MapReduceKeyFilter> list = Arrays.asList(Predicates.and(
					Arrays.asList(Transform.stringToInt(),
							Predicates.greaterThan(0)),
					Arrays.asList(Predicates.lessThan("4"))));
			assertFilters(list, 3);

			list = Arrays.asList(Predicates.or(Arrays.asList(
					Transform.stringToInt(), Predicates.equal(1L)), Arrays
					.asList(Predicates.equal("2"))));
			assertFilters(list, 2);

			list = Arrays.asList(Predicates.and(Arrays.asList(
					Transform.stringToInt(), Predicates.lessThan(4)),
					Arrays.asList(Predicates.not(Arrays.asList(Predicates
							.equal("2"))))));
			assertFilters(list, 3);

		} finally {
			for (int i = 0; i < 5; i++) {
				delete(String.valueOf(i));
			}
		}
	}
}
