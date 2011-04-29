package org.handwerkszeug.riak.mapreduce;

import static org.junit.Assert.assertEquals;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.mapreduce.MapReduceKeyFilters.Predicates;
import org.handwerkszeug.riak.mapreduce.MapReduceKeyFilters.Transform;
import org.handwerkszeug.riak.model.Erlang;
import org.handwerkszeug.riak.util.JsonUtil;
import org.junit.Before;
import org.junit.Test;

public class DefaultMapReduceQueryTest {

	DefaultMapReduceQuery target;

	static JsonNode read(String name) {
		return JsonUtil.read(DefaultMapReduceQueryTest.class, name);
	}

	@Before
	public void setUp() throws Exception {
		this.target = new DefaultMapReduceQuery();
	}

	@Test
	public void testPrepareBucket() throws Exception {
		this.target.setInputs(MapReduceInputs.bucket("testPrepareBucket"));
		this.target.setQueries(NamedFunctionPhase.map(Erlang.map_object_value));
		assertJson("testPrepareBucket");
	}

	protected void assertJson(String expectectedJsonFile) {
		ObjectNode act = this.target.prepare();
		JsonNode exp = read(expectectedJsonFile);
		assertEquals(exp, act);
	}

	@Test
	public void testPrepareMapReduceInput() throws Exception {
		this.target.setInputs(MapReduceInputs.keyFilter("rawBucket",
				Transform.stringToInt(), Predicates.between(10, 20)));
		this.target.setQueries(NamedFunctionPhase.map(Erlang.map_object_value));
		assertJson("testPrepareLocationInput");
	}
}
