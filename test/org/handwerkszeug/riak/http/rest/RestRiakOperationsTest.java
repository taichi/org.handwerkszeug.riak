package org.handwerkszeug.riak.http.rest;

import static org.junit.Assert.assertEquals;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.handwerkszeug.riak.Hosts;
import org.handwerkszeug.riak.model.Link;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.op.RiakOperations;
import org.handwerkszeug.riak.op.RiakOperationsTest;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.junit.Test;

public class RestRiakOperationsTest extends RiakOperationsTest {

	RestRiakOperations target;

	@Override
	protected ChannelPipelineFactory newChannelPipelineFactory() {
		return new RestPipelineFactory();
	}

	@Override
	protected RiakOperations newTarget(Channel channel) {
		this.target = new RestRiakOperations(Hosts.RIAK_URL, "riak", channel);
		return this.target;
	}

	@Override
	protected SocketAddress connectTo() {
		return Hosts.RIAK_HTTP_ADDR;
	}

	@Test
	public void testParseLink() throws Exception {
		String link = "</riak/hb/second>; riaktag=\"foo\", </riak/hb/third>; riaktag=\"bar\", </riak/hb>; rel=\"up\"";
		List<String> links = new ArrayList<String>();
		links.add(link);
		List<Link> actual = target.parse(links);

		assertEquals(2, actual.size());
		Link second = actual.get(0);
		assertEquals(new Location("hb", "second"), second.getLocation());
		assertEquals("foo", second.getTag());

		Link third = actual.get(1);
		assertEquals(new Location("hb", "third"), third.getLocation());
		assertEquals("bar", third.getTag());
	}
}
