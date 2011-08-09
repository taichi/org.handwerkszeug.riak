package org.handwerkszeug.riak.ease;

import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.greaterThan;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.stringToInt;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak.model.Bucket;
import org.handwerkszeug.riak.model.JavaScript;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.Quorum;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.op.RiakOperations;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public abstract class RiakTest<OP extends RiakOperations> {

	Riak<OP> target;

	@Before
	public void setUp() throws Exception {
		this.target = newTarget();
	}

	@After
	public void tearDown() throws Exception {
		this.target.dispose();
	}

	protected abstract Riak<OP> newTarget();

	@Test
	public void testPing() {
		assertEquals("pong", this.target.ping().execute());
	}

	@Test
	public void testPutGetDelete() {
		Location location = new Location("testGet", "key");
		String data = String.valueOf(Math.random()) + "data";

		this.target.put(location, data).execute();

		RiakObject<byte[]> ro = this.target.get(location).execute();
		assertEquals(data, new String(ro.getContent()));

		this.target.delete(location).execute();

		try {
			this.target.get(location).execute();
			fail();
		} catch (RiakException e) {
			assertTrue(true);
		}
	}

	@Test
	public void testPost() throws Exception {
		String bucket = "testPost";
		String data = String.valueOf(Math.random()) + "data";

		RiakObject<byte[]> actual = this.target.post(bucket, data).execute();
		assertPost(bucket, data, actual);

		RiakObject<byte[]> actual2 = this.target.post(bucket, data)
				.setWriteQuorum(Quorum.Default)
				.setDurableWriteQuorum(Quorum.Default).setReturnBody(true)
				.execute();
		assertPost(bucket, data, actual2);

	}

	protected void assertPost(String bucket, String data,
			RiakObject<byte[]> actual) {
		assertNotNull(actual);

		Location location = actual.getLocation();
		assertNotNull(location);

		try {
			assertEquals(data, new String(actual.getContent()));
			assertEquals(bucket, location.getBucket());
			assertNotNull(location.getKey());
			assertFalse(location.getKey().isEmpty());
		} finally {
			this.target.delete(location);
		}
	}

	@Test
	public void testListKeys() {
		String bucket = "testListKeys";
		int size = 10;
		List<String> expected = new ArrayList<String>();
		for (int i = 0; i < size; i++) {
			String k = Integer.toString(i);
			Location location = new Location(bucket, k);
			this.target.put(location, "testListKeys " + i).execute();
			expected.add(k);
		}

		List<String> actuals = this.target.listKeys(bucket).execute();
		Collections.sort(actuals);
		assertEquals(expected, actuals);

		for (int i = 0; i < size; i++) {
			Location location = new Location(bucket, Integer.toString(i));
			this.target.delete(location).execute();
		}
	}

	@Test
	public void testGetSetBucket() throws Exception {
		String bucket = "testGetBucket";
		Location location = new Location(bucket, "key");
		this.target.put(location, "data").execute();

		Bucket actual = this.target.getBucket(bucket).execute();
		assertNotNull(actual);
		assertEquals(bucket, actual.getName());

		actual.setNumberOfReplicas(5);
		this.target.setBucket(actual).execute();

		Bucket actual2 = this.target.getBucket(bucket).execute();
		assertEquals(actual.getNumberOfReplicas(),
				actual2.getNumberOfReplicas());

		this.target.delete(location).execute();
	}

	@Test
	public void testMapReduce() throws Exception {
		String bucket = "testMapReduce";
		for (int i = 0; i < 5; i++) {
			Location loc = new Location(bucket, String.valueOf(i));
			this.target.put(loc, "data").execute();
		}
		List<ArrayNode> list = this.target.mapReduce().inputs(bucket)
				.keyFilters(stringToInt, greaterThan(2))
				.map(JavaScript.mapValues, true).execute();

		assertEquals(2, list.size());
		ArrayNode an = list.get(0);
		JsonNode jn = an.get(0);
		assertEquals("data", jn.getTextValue());

		for (int i = 0; i < 5; i++) {
			Location loc = new Location(bucket, String.valueOf(i));
			this.target.delete(loc).execute();
		}
	}
}
