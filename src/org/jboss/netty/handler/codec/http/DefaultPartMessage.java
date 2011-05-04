package org.jboss.netty.handler.codec.http;

/**
 * @author taichi
 */
public class DefaultPartMessage extends DefaultHttpMessage implements
		PartMessage {

	boolean last;

	public DefaultPartMessage() {
		super(HttpVersion.HTTP_1_1);
	}

	@Override
	public boolean isLast() {
		return this.last;
	}

	public void setLast(boolean last) {
		this.last = last;
	}
}
