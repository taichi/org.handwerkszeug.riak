package org.jboss.netty.handler.codec.http;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class MultipartChunkedResponseDecoderTest {

	MultipartChunkedResponseDecoder target;

	@Before
	public void setUp() throws Exception {
		target = new MultipartChunkedResponseDecoder();
	}

	@Test
	public void testGetBoundary() throws Exception {
		String s = "multipart/mixed; boundary=YinLMzyUR9feB17okMytgKsylvh";
		DefaultHttpResponse response = new DefaultHttpResponse(
				HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		response.setHeader(HttpHeaders.Names.CONTENT_TYPE, s);
		String b = target.getBoundary(response);
		assertEquals("--YinLMzyUR9feB17okMytgKsylvh", b);
	}
}
