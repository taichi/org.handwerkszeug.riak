package org.handwerkszeug.riak.model;

import java.util.Arrays;

/**
 * @author taichi
 */
public class DefaultRiakObject extends AbstractRiakObject<byte[]> {

	private byte[] content;

	public DefaultRiakObject(Location location) {
		super(location);

	}

	@Override
	public byte[] getContent() {
		return this.content;
	}

	public void setContent(byte[] content) {
		this.content = content;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("DefaultRiakObject [location=");
		builder.append(this.getLocation());
		builder.append(", content=");
		builder.append(Arrays.toString(this.content));
		builder.append(", vectorClock=");
		builder.append(this.getVectorClock());
		builder.append(", contentType=");
		builder.append(this.getContentType());
		builder.append(", charset=");
		builder.append(this.getCharset());
		builder.append(", contentEncoding=");
		builder.append(this.getContentEncoding());
		builder.append(", vtag=");
		builder.append(this.getVtag());
		builder.append(", links=");
		builder.append(this.getLinks());
		builder.append(", lastModified=");
		builder.append(this.getLastModified());
		builder.append(", userMetadata=");
		builder.append(this.getUserMetadata());
		builder.append("]");
		return builder.toString();
	}

}
