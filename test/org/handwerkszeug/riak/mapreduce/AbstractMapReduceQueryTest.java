package org.handwerkszeug.riak.mapreduce;

import org.codehaus.jackson.node.ObjectNode;
import org.junit.Before;
import org.junit.Test;

public class AbstractMapReduceQueryTest {

	AbstractMapReduceQuery target;

	@Before
	public void setUp() throws Exception {
		this.target = new AbstractMapReduceQuery() {
		};
	}

	@Test
	public void testPrepareBucket() throws Exception {
		this.target.setInputs("testbucket");
		ObjectNode act = this.target.prepare();
		System.out.println(act);
	}

}
