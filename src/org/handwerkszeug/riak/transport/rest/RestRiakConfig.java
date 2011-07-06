package org.handwerkszeug.riak.transport.rest;

import java.net.InetSocketAddress;

import org.handwerkszeug.riak.config.DefaultRiakConfig;

/**
 * @author taichi
 */
public class RestRiakConfig extends DefaultRiakConfig {

	public RestRiakConfig(InetSocketAddress address) {
		super(address);
	}

	public String getRawName() {
		return "riak";
	}

	public String getMapReduceName() {
		return "mapred";
	}

	public String getLuwakName() {
		return "luwak";
	}

	public static RestRiakConfig newConfig(String host) {
		return newConfig(host, 8098);
	}

	public static RestRiakConfig newConfig(String host, int port) {
		return new RestRiakConfig(new InetSocketAddress(host, port));
	}
}
