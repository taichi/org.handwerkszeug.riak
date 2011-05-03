package org.jboss.netty.handler.codec.http;

import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.DefaultHttpMessage;
import org.jboss.netty.handler.codec.http.HttpVersion;

/**
 * @author taichi
 */
public class DefaultMultiPartChunk implements MultiPartChunk {

	DefaultHttpMessage delegate;
	boolean last;

	public DefaultMultiPartChunk() {
		this.delegate = new DefaultHttpMessage(HttpVersion.HTTP_1_1) {
		};
	}

	@Override
	public boolean isLast() {
		return this.last;
	}

	public void setLast(boolean last) {
		this.last = last;
	}

	@Override
	public void addHeader(String name, Object value) {
		delegate.addHeader(name, value);
	}

	@Override
	public int hashCode() {
		return delegate.hashCode();
	}

	@Override
	public void setHeader(String name, Object value) {
		delegate.setHeader(name, value);
	}

	@Override
	public void setHeader(String name, Iterable<?> values) {
		delegate.setHeader(name, values);
	}

	@Override
	public void removeHeader(String name) {
		delegate.removeHeader(name);
	}

	public void clearHeaders() {
		delegate.clearHeaders();
	}

	@Override
	public void setContent(ChannelBuffer content) {
		delegate.setContent(content);
	}

	@Override
	public String getHeader(String name) {
		return delegate.getHeader(name);
	}

	@Override
	public List<String> getHeaders(String name) {
		return delegate.getHeaders(name);
	}

	@Override
	public List<Entry<String, String>> getHeaders() {
		return delegate.getHeaders();
	}

	@Override
	public boolean containsHeader(String name) {
		return delegate.containsHeader(name);
	}

	@Override
	public Set<String> getHeaderNames() {
		return delegate.getHeaderNames();
	}

	@Override
	public ChannelBuffer getContent() {
		return delegate.getContent();
	}

}
