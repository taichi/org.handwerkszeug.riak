package org.handwerkszeug.riak.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @author taichi
 */
public class DefaultRiakObject extends AbstractRiakObject<byte[]> {

	private byte[] content;

	public DefaultRiakObject(Location location) {
		super(location);
	}

	public DefaultRiakObject(RiakObject<byte[]> src) {
		super(src.getLocation());
		setContent(Arrays.copyOf(src.getContent(), src.getContent().length));
		setVectorClock(src.getVectorClock());
		setContentType(src.getContentType());
		setCharset(src.getCharset());
		setContentEncoding(src.getContentEncoding());
		setVtag(src.getVtag());
		List<Link> links = src.getLinks();
		if (links != null && links.isEmpty() == false) {
			setLinks(new ArrayList<Link>(links));
		}
		Date d = src.getLastModified();
		if (d != null) {
			setLastModified(new Date(d.getTime()));
		}
		Map<String, String> metas = src.getUserMetadata();
		if (metas != null && metas.isEmpty() == false) {
			setUserMetadata(new HashMap<String, String>(metas));
		}
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
