package org.handwerkszeug.riak.transport.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.JavaType;
import org.handwerkszeug.riak.transport.rest.JsonBucket;
import org.handwerkszeug.riak.transport.rest.internal.BucketHolder;
import org.handwerkszeug.riak.util.JsonUtil;
import org.junit.Before;
import org.junit.Test;

/**
 * @author taichi
 */
public class JsonBucketTest {

	ObjectMapper om;

	@Before
	public void setUp() {
		om = new ObjectMapper();
	}

	@Test
	public void test() throws Exception {
		JsonNode expected = JsonUtil.read(JsonBucketTest.class, "test");
		System.out.println(expected);
		JsonBucket bucket = om.readValue(expected, JsonBucket.class);
		System.out.println(bucket);
		JsonNode actual = to(bucket);
		System.out.println(actual);
		assertEquals(expected, actual);
	}

	@Test
	public void testNull() throws Exception {
		JsonNode expected = JsonUtil.read(JsonBucketTest.class, "null");
		JsonBucket bucket = om.readValue(expected, JsonBucket.class);
		assertNull(bucket.getBackend());
		JsonNode actual = to(bucket);
		assertEquals(expected, actual);

	}

	@Test
	public void testWithIgnore() throws Exception {
		JsonNode ignore = JsonUtil.read(JsonBucketTest.class, "withIgnore");
		JsonBucket bucket = om.readValue(ignore, JsonBucket.class);

		JsonNode actual = to(bucket);
		JsonNode expected = JsonUtil.read(JsonBucketTest.class, "test");

		assertEquals(expected, actual);
	}

	protected JsonNode to(Object value) throws IOException,
			JsonGenerationException, JsonMappingException,
			JsonProcessingException {
		JsonFactory factory = new JsonFactory();
		StringWriter sw = new StringWriter();
		JsonGenerator jgen = factory.createJsonGenerator(sw);
		om.writeValue(jgen, value);
		return om.readTree(new StringReader(sw.toString()));
	}

	@Test
	public void testProps() throws Exception {
		JsonNode eProps = JsonUtil.read(JsonBucketTest.class, "props");
		JavaType jt = om.getTypeFactory().constructType(BucketHolder.class);
		BucketHolder holder = om.readValue(eProps, jt);

		JsonNode expected = JsonUtil.read(JsonBucketTest.class, "test");
		JsonNode actual = to(holder.props);

		assertEquals(actual, expected);

		JsonNode aProps = to(holder);

		assertEquals(eProps, aProps);
		System.out.println(aProps);

	}
}
