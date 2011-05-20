package org.handwerkszeug.riak.http.rest;

import java.net.InetSocketAddress;

import org.handwerkszeug.riak.config.AbstractConfig;

/**
 * @author taichi
 */
public class RestConfig extends AbstractConfig {

	public RestConfig(InetSocketAddress address) {
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

	public static RestConfig newConfig(String host) {
		return newConfig(host, 8098);
	}

	public static RestConfig newConfig(String host, int port) {
		return new RestConfig(new InetSocketAddress(host, port));
	}
}
