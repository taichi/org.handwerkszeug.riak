package org.handwerkszeug.riak.http.rest;

import static org.junit.Assert.assertEquals;

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
import org.handwerkszeug.riak.model.Bucket;
import org.handwerkszeug.riak.util.JsonUtil;
import org.junit.Test;

/**
 * @author taichi
 */
public class JsonBucketTest {

	@Test
	public void test() throws Exception {
		JsonNode expected = JsonUtil.read(JsonBucketTest.class, "test");
		System.out.println(expected);
		ObjectMapper om = new ObjectMapper();
		JsonBucket bucket = om.readValue(expected, JsonBucket.class);
		System.out.println(bucket);
		JsonNode actual = to(om, bucket);
		System.out.println(actual);
		assertEquals(expected, actual);
	}

	@Test
	public void testWithIgnore() throws Exception {
		JsonNode ignore = JsonUtil.read(JsonBucketTest.class, "withIgnore");
		ObjectMapper om = new ObjectMapper();
		JsonBucket bucket = om.readValue(ignore, JsonBucket.class);

		JsonNode actual = to(om, bucket);
		JsonNode expected = JsonUtil.read(JsonBucketTest.class, "test");

		assertEquals(expected, actual);
	}

	protected JsonNode to(ObjectMapper om, Bucket bucket) throws IOException,
			JsonGenerationException, JsonMappingException,
			JsonProcessingException {
		JsonFactory factory = new JsonFactory();
		StringWriter sw = new StringWriter();
		JsonGenerator jgen = factory.createJsonGenerator(sw);
		om.writeValue(jgen, bucket);
		JsonNode actual = om.readTree(new StringReader(sw.toString()));
		return actual;
	}

	@Test
	public void testProps() throws Exception {
		JsonNode node = JsonUtil.read(JsonBucketTest.class, "props");
		ObjectMapper om = new ObjectMapper();
		JavaType jt = om.getTypeFactory().constructType(BucketHolder.class);
		BucketHolder holder = om.readValue(node, jt);

		JsonNode expected = JsonUtil.read(JsonBucketTest.class, "test");
		JsonNode actual = to(om, holder.props);

		assertEquals(actual, expected);

	}
}
