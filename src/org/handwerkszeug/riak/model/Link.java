package org.handwerkszeug.riak.model;

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
}