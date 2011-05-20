package org.handwerkszeug.riak.pbc;

import java.net.InetSocketAddress;

import org.handwerkszeug.riak.config.AbstractConfig;

/**
 * @author taichi
 */
public class PbcConfig extends AbstractConfig {

	public PbcConfig(InetSocketAddress address) {
		super(address);
	}

	public static PbcConfig newConfig(String host) {
		return newConfig(host, 8087);
	}

	public static PbcConfig newConfig(String host, int port) {
		return new PbcConfig(new InetSocketAddress(host, port));
	}
}
