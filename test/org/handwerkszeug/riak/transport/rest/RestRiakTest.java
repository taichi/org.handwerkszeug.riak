package org.handwerkszeug.riak.transport.rest;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.handwerkszeug.riak.Hosts;
import org.handwerkszeug.riak.ease.RiakTest;
import org.handwerkszeug.riak.model.DefaultRiakObject;
import org.handwerkszeug.riak.model.Link;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.RiakObject;
import org.junit.Test;

/**
 * @author taichi
 */
public class RestRiakTest extends RiakTest<RestRiakOperations, RestRiak> {

	@Override
	protected RestRiak newTarget() {
		return RestRiak.create(Hosts.RIAK_HOST);
	}

	@Test
	public void testLinkWalking() throws Exception {
		// A -> B -> D
		// A -> C -> D

		String bucket = "testWalk";

		RiakObject<byte[]> D = createData(bucket, "D", new ArrayList<Link>());
		put(D);

		List<Link> c2d = new ArrayList<Link>();
		c2d.add(new Link(D.getLocation(), "2d"));
		RiakObject<byte[]> C = createData(bucket, "C", c2d);
		put(C);

		List<Link> b2d = new ArrayList<Link>();
		b2d.add(new Link(D.getLocation(), "2d"));
		RiakObject<byte[]> B = createData(bucket, "B", b2d);
		put(B);

		List<Link> a = new ArrayList<Link>();
		a.add(new Link(C.getLocation(), "a2c"));
		a.add(new Link(B.getLocation(), "a2b"));
		RiakObject<byte[]> A = createData(bucket, "A", a);
		put(A);

		List<List<byte[]>> expected = new ArrayList<List<byte[]>>();
		List<byte[]> phase1 = new ArrayList<byte[]>();
		phase1.add(B.getContent());
		phase1.add(C.getContent());
		expected.add(phase1);

		List<byte[]> phase2 = new ArrayList<byte[]>();
		phase2.add(D.getContent());
		expected.add(phase2);

		try {
			List<List<RiakObject<byte[]>>> actuals = this.target
					.walk(A.getLocation()).step(LinkCondition.KEEP_ANY)
					.step(LinkCondition.ANY).execute();

			assertEquals(expected.size(), actuals.size());
			assertROEquals(phase1, actuals.get(0));
			assertROEquals(phase2, actuals.get(1));

		} finally {
			// delete
			delete(D);
			delete(C);
			delete(B);
			delete(A);
		}
	}

	void assertROEquals(List<byte[]> expected, List<RiakObject<byte[]>> actuals) {
		List<String> expString = toString(expected);
		List<String> actString = toStringRO(actuals);
		Collections.sort(expString);
		Collections.sort(actString);
		assertEquals(expString, actString);
	}

	List<String> toString(List<byte[]> from) {
		List<String> result = new ArrayList<String>();
		for (byte[] ary : from) {
			result.add(new String(ary));
		}
		return result;
	}

	List<String> toStringRO(List<RiakObject<byte[]>> from) {
		List<String> result = new ArrayList<String>();
		for (RiakObject<byte[]> ro : from) {
			result.add(new String(ro.getContent()));
		}
		return result;
	}

	RiakObject<byte[]> createData(String bucket, String key, List<Link> links)
			throws Exception {
		Location location = new Location(bucket, key);
		DefaultRiakObject ro = new DefaultRiakObject(location);
		ro.setLinks(links);

		String data = bucket + key + new Date();
		ro.setContent(data.getBytes());
		return ro;
	}

	void put(RiakObject<byte[]> ro) throws Exception {
		this.target.put(ro).execute();
	}

	void delete(RiakObject<byte[]> ro) throws Exception {
		this.target.delete(ro.getLocation()).execute();
	}
}
