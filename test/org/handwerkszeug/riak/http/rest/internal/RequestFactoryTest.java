package org.handwerkszeug.riak.http.rest.internal;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.handwerkszeug.riak.model.Link;
import org.handwerkszeug.riak.model.Location;
import org.junit.Before;
import org.junit.Test;

public class RequestFactoryTest {

	RequestFactory target;

	@Before
	public void setUp() throws Exception {
		this.target = new RequestFactory("", null);
	}

	@Test
	public void testParseLink() throws Exception {
		String link = "</riak/hb/second>; riaktag=\"foo\", </riak/hb/third>; riaktag=\"bar\", </riak/hb>; rel=\"up\"";
		List<String> links = new ArrayList<String>();
		links.add(link);
		List<Link> actual = target.parse(links);

		assertEquals(2, actual.size());
		Link second = actual.get(0);
		assertEquals(new Location("hb", "second"), second.getLocation());
		assertEquals("foo", second.getTag());

		Link third = actual.get(1);
		assertEquals(new Location("hb", "third"), third.getLocation());
		assertEquals("bar", third.getTag());
	}
}
