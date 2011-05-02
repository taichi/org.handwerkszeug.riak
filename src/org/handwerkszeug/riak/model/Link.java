package org.handwerkszeug.riak.model;

/**
 * @author taichi
 */
public class Link {

	final Location location;

	final String tag;

	public Link(Location location, String tag) {
		this.location = location;
		this.tag = tag;
	}

	public Location getLocation() {
		return this.location;
	}

	public String getTag() {
		return this.tag;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Link [location=");
		builder.append(location);
		builder.append(", tag=");
		builder.append(tag);
		builder.append("]");
		return builder.toString();
	}

}
