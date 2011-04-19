package org.handwerkszeug.riak.mapreduce;

import static org.junit.Assert.assertEquals;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.util.JsonUtil;
import org.junit.Before;
import org.junit.Test;

public class AbstractMapReduceQueryTest {

	AbstractMapReduceQuery target;

	static JsonNode read(String name) {
		return JsonUtil.read(AbstractMapReduceQueryTest.class, name);
	}

	@Before
	public void setUp() throws Exception {
		this.target = new AbstractMapReduceQuery() {
		};
	}

	@Test
	public void testPrepareBucket() throws Exception {
		this.target.setInputs("testPrepareBucket");
		ObjectNode act = this.target.prepare();
		JsonNode exp = read("testPrepareBucket");
		assertEquals(exp, act);

	}

}
