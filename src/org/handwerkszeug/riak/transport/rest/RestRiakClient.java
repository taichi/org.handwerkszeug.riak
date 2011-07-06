package org.handwerkszeug.riak.transport.rest;

import org.handwerkszeug.riak.transport.internal.AbstractRiakClient;
import org.handwerkszeug.riak.transport.rest.internal.RestPipelineFactory;
import org.jboss.netty.channel.Channel;

/**
 * @author taichi
 */
public class RestRiakClient extends
		AbstractRiakClient<RestRiakConfig, RestRiakOperations> {

	final String riakUri;

	public RestRiakClient(RestRiakConfig config) {
		super(config, new RestPipelineFactory());
		this.riakUri = toRiakURI(config);
	}

	public static String toRiakURI(RestRiakConfig config) {
		StringBuilder stb = new StringBuilder();
		stb.append("http://");
		stb.append(config.getRiakAddress().getHostName());
		stb.append(':');
		stb.append(config.getRiakAddress().getPort());
		return stb.toString();
	}

	@Override
	protected RestRiakOperations newOperations(Channel channel) {
		return new RestRiakOperations(this.riakUri, this.config, channel);
	}
}
