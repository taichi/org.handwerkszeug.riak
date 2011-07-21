package org.handwerkszeug.riak.ease;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak.model.Location;
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
		assertNotNull(this.target.ping());
	}

	@Test
	public void testPutGet() {
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
}
