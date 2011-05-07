package org.handwerkszeug.riak.config;

import java.net.InetSocketAddress;

import org.handwerkszeug.riak.Config;

/**
 * @author taichi
 */
public class DefaultConfig extends AbstractConfig {

	final InetSocketAddress address;

	protected DefaultConfig(InetSocketAddress address) {
		this.address = address;
	}

	@Override
	public InetSocketAddress getRiakAddress() {
		return this.address;
	}

	public static Config newRestConfig(String host) {
		return newConfig(host, 8098);
	}

	public static Config newPbcConfig(String host) {
		return newConfig(host, 8087);
	}

	public static Config newConfig(String host, int port) {
		return new DefaultConfig(new InetSocketAddress(host, port));
	}
}
