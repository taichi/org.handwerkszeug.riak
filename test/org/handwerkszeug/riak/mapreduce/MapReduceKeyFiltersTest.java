package org.handwerkszeug.riak.mapreduce;

import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.and;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.between;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.endsWith;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.equal;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.filters;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.floatToString;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.greaterThan;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.greaterThanEq;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.intToString;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.lessThan;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.lessThanEq;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.matches;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.not;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.notEqual;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.or;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.setMember;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.similarTo;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.startsWith;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.stringToFloat;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.stringToInt;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.toLower;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.toUpper;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.tokenize;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.urldecode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.handwerkszeug.riak.Hosts;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.model.DefaultRiakObject;
import org.handwerkszeug.riak.model.JavaScript;
import org.handwerkszeug.riak.model.KeyResponse;
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

/**
 * @author taichi
 */
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
		deleteFromBucket();
	}

	@After
	public void tearDown() throws Exception {
		this.channel.close().awaitUninterruptibly();
		this.bootstrap.releaseExternalResources();
	}

	void waitFor(RiakFuture rf) throws Exception {
		assertTrue("timeout.", rf.await(5, TimeUnit.SECONDS));
	}

	void deleteFromBucket() throws Exception {
		final List<String> keys = new ArrayList<String>();
		RiakFuture rf = this.target.listKeys(TEST_BUCKET,
				new TestingHandler<KeyResponse>() {
					@Override
					public void handle(
							RiakContentsResponse<KeyResponse> response)
							throws Exception {
						KeyResponse kr = response.getContents();
						keys.addAll(kr.getKeys());
					}
				});
		waitFor(rf);
		for (String s : keys) {
			delete(s);
		}
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

	void assertFilters(int expectedCount, MapReduceKeyFilter primary,
			final MapReduceKeyFilter... keyFilters) throws Exception {
		final List<String> list = new ArrayList<String>();

		MapReduceQueryBuilder<RiakFuture> builder = this.target
				.mapReduce(new TestingHandler<MapReduceResponse>() {
					@Override
					public void handle(
							RiakContentsResponse<MapReduceResponse> response)
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

		RiakFuture rf = builder.inputs(TEST_BUCKET)
				.keyFilters(primary, keyFilters).map(JavaScript.mapValues)
				.execute();

		waitFor(rf);
		System.out.println(list);
		assertEquals(expectedCount, list.size());
	}

	@Test
	public void testTransformInt() throws Exception {
		for (int i = 0; i < 5; i++) {
			put(String.valueOf(i));
		}
		assertFilters(3, stringToInt, intToString, stringToInt, greaterThan(1));

		assertFilters(3, stringToInt, greaterThan(1L));
		assertFilters(3, stringToInt, greaterThan(BigInteger.valueOf(1L)));
		assertFilters(3, stringToInt, lessThan(3));
		assertFilters(3, stringToInt, lessThan(3L));
		assertFilters(3, stringToInt, lessThan(BigInteger.valueOf(3L)));
		assertFilters(3, stringToInt, greaterThanEq(2));
		assertFilters(3, stringToInt, greaterThanEq(2L));
		assertFilters(3, stringToInt, greaterThanEq(BigInteger.valueOf(2L)));
		assertFilters(3, stringToInt, lessThanEq(2));
		assertFilters(3, stringToInt, lessThanEq(2L));
		assertFilters(3, stringToInt, lessThanEq(BigInteger.valueOf(2L)));
		assertFilters(4, stringToInt, notEqual(3));
		assertFilters(4, stringToInt, notEqual(3L));
		assertFilters(4, stringToInt, notEqual(BigInteger.valueOf(3L)));
		assertFilters(1, stringToInt, equal(2));
		assertFilters(1, stringToInt, equal(2L));
		assertFilters(1, stringToInt, equal(BigInteger.valueOf(2L)));
		assertFilters(3, stringToInt, between(1, 3));
		assertFilters(1, stringToInt, between(1, 3, false));
		assertFilters(3, stringToInt, between(1L, 3L));
		assertFilters(1, stringToInt, between(1L, 3L, false));
		assertFilters(3, stringToInt,
				between(BigInteger.valueOf(1L), BigInteger.valueOf(3L)));
		assertFilters(1, stringToInt,
				between(BigInteger.valueOf(1L), BigInteger.valueOf(3L), false));
		assertFilters(3, stringToInt, setMember(1, 2, 3));
		assertFilters(2, stringToInt, setMember(0L, 4L));
		assertFilters(2, stringToInt,
				setMember(BigInteger.valueOf(0L), BigInteger.valueOf(4L)));
	}

	@Test
	public void testTransformFloat() throws Exception {
		for (int i = 0; i < 5; i++) {
			put(Float.toString(i));
		}

		assertFilters(3, stringToFloat, floatToString, stringToFloat,
				greaterThan(1.0f));
		assertFilters(3, stringToFloat, greaterThan(1.0));
		assertFilters(3, stringToFloat, greaterThan(BigDecimal.valueOf(1.0)));
		assertFilters(3, stringToFloat, lessThan(3.0));
		assertFilters(3, stringToFloat, lessThan(3.0f));
		assertFilters(3, stringToFloat, lessThan(BigDecimal.valueOf(3.0)));
		assertFilters(3, stringToFloat, greaterThanEq(2.0));
		assertFilters(3, stringToFloat, greaterThanEq(2.0f));
		assertFilters(3, stringToFloat, greaterThanEq(BigDecimal.valueOf(2.0f)));
		assertFilters(3, stringToFloat, lessThanEq(2.0));
		assertFilters(3, stringToFloat, lessThanEq(2.0f));
		assertFilters(3, stringToFloat, lessThanEq(BigDecimal.valueOf(2.0f)));
		assertFilters(4, stringToFloat, notEqual(3.0));
		assertFilters(4, stringToFloat, notEqual(3.0f));
		assertFilters(4, stringToFloat, notEqual(BigDecimal.valueOf(3.0f)));
		assertFilters(1, stringToFloat, equal(2.0));
		assertFilters(1, stringToFloat, equal(2.0f));
		assertFilters(1, stringToFloat, equal(BigDecimal.valueOf(2.0f)));
		assertFilters(3, stringToFloat, between(1.0, 3.0));
		assertFilters(1, stringToFloat, between(1.0, 3.0, false));
		assertFilters(3, stringToFloat, between(1.0f, 3.0f));
		assertFilters(1, stringToFloat, between(1.0f, 3.0f, false));
		assertFilters(3, stringToFloat,
				between(BigDecimal.valueOf(1.0f), BigDecimal.valueOf(3.0f)));
		assertFilters(
				1,
				stringToFloat,
				between(BigDecimal.valueOf(1.0f), BigDecimal.valueOf(3.0f),
						false));
		assertFilters(2, stringToFloat, setMember(1.0f, 3.0f));
		assertFilters(3, stringToFloat, setMember(1.0, 3.0, 4.0));
		assertFilters(
				3,
				stringToFloat,
				setMember(BigDecimal.valueOf(1.0), BigDecimal.valueOf(3.0),
						BigDecimal.valueOf(4.0)));

	}

	@Test
	public void testTransformString() throws Exception {
		String[] ary = { "a,0", "b,1", "c,2", "d,3", "e,4" };
		for (String s : ary) {
			put(s);
		}
		assertFilters(3, tokenize(",", 2), stringToInt, greaterThanEq(2));
		assertFilters(3, toLower, toUpper, tokenize(",", 1), greaterThan("B"));
	}

	@Test
	public void testStringSimilarity() throws Exception {
		String[] ary = { "1a2", "1b3", "2c4", "2d4", "3e5" };
		for (String s : ary) {
			put(s);
		}
		assertFilters(3, greaterThan("2c"));
		assertFilters(2, lessThan("2c"));
		assertFilters(2, greaterThanEq("2d4"));
		assertFilters(2, lessThanEq("1b3"));
		assertFilters(4, notEqual("1b3"));
		assertFilters(1, equal("1b3"));
		assertFilters(4, between("1b3", "3e5"));
		assertFilters(2, between("1b3", "3e5", false));
		assertFilters(1, startsWith("1a"));
		assertFilters(2, endsWith("4"));
		assertFilters(2, similarTo("2a4", 1));
		assertFilters(3, setMember("1a2", "2c4", "3e5"));
		assertFilters(4, matches("[12][a-z]\\d+"));
	}

	@Test
	public void testUrlDecode() throws Exception {
		String[] ary = { "1%2F2", "1%2F3", "2%2F4", "3%2F4", "3%2F5" };
		for (String s : ary) {
			put(s);
		}

		assertFilters(2, urldecode, tokenize("/", 1), stringToInt,
				greaterThan(2));
	}

	@Test
	public void testLogicalFilters() throws Exception {
		for (int i = 0; i < 5; i++) {
			put(String.valueOf(i));
		}

		assertFilters(
				3,
				and(filters(stringToInt, greaterThan(0)),
						filters(lessThan("4"))));
		assertFilters(2,
				or(filters(stringToInt, equal(1L)), filters(equal("2"))));
		assertFilters(
				3,
				and(filters(stringToInt, lessThan(4)),
						filters(not(filters(equal("2"))))));
	}
}
