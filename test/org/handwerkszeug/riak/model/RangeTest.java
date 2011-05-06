package org.handwerkszeug.riak.model;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class RangeTest {

	@Test
	public void testRange() {
		Range r = Range.range(10, 5000);
		assertEquals("bytes=10-5000", r.toString());
	}

	@Test
	public void testFirst() {
		Range r = Range.first(501);
		assertEquals("bytes=501", r.toString());
	}

	@Test
	public void testLast() {
		Range r = Range.last(500);
		assertEquals("bytes=-500", r.toString());
	}

	@Test
	public void testRanges() {
		Range r = Range.ranges(Range.range(10, 20), Range.range(41, 300));
		assertEquals("bytes=10-20,41-300", r.toString());
	}

}
