package org.handwerkszeug.riak.util;

import static org.junit.Assert.assertEquals;

import java.util.Date;

import org.junit.Test;

/**
 * @author taichi
 */
public class HttpUtilTest {

	@Test
	public void testParse() {
		String exp = "Sun, 01 May 2011 17:39:44 GMT";
		Date d = HttpUtil.parse(exp);
		String act = HttpUtil.format(d);
		assertEquals(exp, act);
	}
}
