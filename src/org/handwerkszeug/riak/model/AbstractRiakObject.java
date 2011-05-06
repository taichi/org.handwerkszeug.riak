package org.handwerkszeug.riak.model;

import static org.handwerkszeug.riak.util.Validation.notNull;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author taichi
 */
public abstract class AbstractRiakObject<T> implements RiakObject<T> {

	private Location location;
	private String vectorClock;
	private String contentType = DEFAULT_CONTENT_TYPE;
	private String charset;
	private String contentEncoding;
	private String vtag;
	private List<Link> links = Collections.emptyList();
	private Date lastModified;
	private Map<String, String> userMetadata = Collections.emptyMap();

	protected AbstractRiakObject() {
	}

	public AbstractRiakObject(Location location) {
		notNull(location, "location");
		this.location = location;
	}

	@Override
	public Location getLocation() {
		return this.location;
	}

	@Override
	public void setLocation(Location location) {
		this.location = location;
	}

	@Override
	public String getVectorClock() {
		return this.vectorClock;
	}

	@Override
	public void setVectorClock(String clock) {
		this.vectorClock = clock;
	}

	@Override
	public String getContentType() {
		return this.contentType;
	}

	@Override
	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	@Override
	public String getCharset() {
		return this.charset;
	}

	@Override
	public void setCharset(String charset) {
		this.charset = charset;
	}

	@Override
	public String getContentEncoding() {
		return this.contentEncoding;
	}

	@Override
	public void setContentEncoding(String encoding) {
		this.contentEncoding = encoding;
	}

	@Override
	public String getVtag() {
		return this.vtag;
	}

	@Override
	public void setVtag(String vtag) {
		this.vtag = vtag;
	}

	@Override
	public List<Link> getLinks() {
		return this.links;
	}

	@Override
	public void setLinks(List<Link> links) {
		this.links = links;
	}

	@Override
	public Date getLastModified() {
		return this.lastModified;
	}

	@Override
	public void setLastModified(Date date) {
		this.lastModified = date;
	}

	@Override
	public Map<String, String> getUserMetadata() {
		return this.userMetadata;
	}

	@Override
	public void setUserMetadata(Map<String, String> metadata) {
		this.userMetadata = metadata;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("AbstractRiakObject [location=");
		builder.append(this.location);
		builder.append(", vectorClock=");
		builder.append(this.vectorClock);
		builder.append(", contentType=");
		builder.append(this.contentType);
		builder.append(", charset=");
		builder.append(this.charset);
		builder.append(", contentEncoding=");
		builder.append(this.contentEncoding);
		builder.append(", vtag=");
		builder.append(this.vtag);
		builder.append(", links=");
		builder.append(this.links);
		builder.append(", lastModified=");
		builder.append(this.lastModified);
		builder.append(", userMetadata=");
		builder.append(this.userMetadata);
		builder.append("]");
		return builder.toString();
	}
}
