package org.handwerkszeug.riak.mapreduce.internal;

import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.between;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.stringToInt;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.model.Erlang;
import org.handwerkszeug.riak.util.JsonUtil;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Before;
import org.junit.Test;

public class MapReduceQueryContextTest {

	MapReduceQueryContext<_> target;

	@Before
	public void setUp() throws Exception {
		this.target = new MapReduceQueryContext<_>() {
			@Override
			public _ execute() {
				return _._;
			}
		};
	}

	static JsonNode read(String name) {
		return JsonUtil.read(MapReduceQueryContextTest.class, name);
	}

	protected void assertJson(String expectedJsonFile)
			throws JsonProcessingException, IOException {
		ChannelBuffer cb = ChannelBuffers.dynamicBuffer();
		this.target.prepare(new ChannelBufferOutputStream(cb));
		ObjectMapper om = new ObjectMapper();
		JsonNode act = om.readTree(new ChannelBufferInputStream(cb));
		JsonNode exp = read(expectedJsonFile);
		assertEquals(exp, act);
	}

	@Test
	public void testPrepareBucket() throws Exception {
		String bucket = "testPrepareBucket";
		this.target.add(new BucketInput(bucket));
		this.target.add(MapReducePhase.PhaseType.map, true,
				Erlang.map_object_value);
		assertJson(bucket);
	}

	@Test
	public void testPrepareKeyFilter() throws Exception {
		String bucket = "testPrepareKeyFilter";
		this.target.add(new BucketInput(bucket));
		this.target.add(stringToInt, between(10, 20));
		this.target.add(MapReducePhase.PhaseType.map, false,
				Erlang.map_object_value);
		assertJson("testPrepareKeyFilter");
	}
}
