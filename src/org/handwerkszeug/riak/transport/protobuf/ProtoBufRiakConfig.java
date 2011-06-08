package org.handwerkszeug.riak.transport.protobuf;

import java.net.InetSocketAddress;

import org.handwerkszeug.riak.config.AbstractConfig;

/**
 * @author taichi
 */
public class ProtoBufRiakConfig extends AbstractConfig {

	public ProtoBufRiakConfig(InetSocketAddress address) {
		super(address);
	}

	public static ProtoBufRiakConfig newConfig(String host) {
		return newConfig(host, 8087);
	}

	public static ProtoBufRiakConfig newConfig(String host, int port) {
		return new ProtoBufRiakConfig(new InetSocketAddress(host, port));
	}
}
