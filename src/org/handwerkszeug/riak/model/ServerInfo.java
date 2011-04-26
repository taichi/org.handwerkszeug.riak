package org.handwerkszeug.riak.model;

/**
 * @author taichi
 */
public class ServerInfo {

	final String node;
	final String serverVersion;

	public ServerInfo(String node, String serverVersion) {
		this.node = node;
		this.serverVersion = serverVersion;
	}

	public String getNode() {
		return this.node;
	}

	public String getServerVersion() {
		return this.serverVersion;
	}
}
