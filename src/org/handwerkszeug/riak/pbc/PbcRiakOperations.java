package org.handwerkszeug.riak.pbc;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.handwerkszeug.riak.Markers;
import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.mapreduce.MapReduceQueryConstructor;
import org.handwerkszeug.riak.mapreduce.MapReduceResponseHandler;
import org.handwerkszeug.riak.model.Bucket;
import org.handwerkszeug.riak.model.DefaultRiakObject;
import org.handwerkszeug.riak.model.Link;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.Quorum;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.model.ServerInfo;
import org.handwerkszeug.riak.nls.Messages;
import org.handwerkszeug.riak.op.GetOptions;
import org.handwerkszeug.riak.op.KeyHandler;
import org.handwerkszeug.riak.op.PutOptions;
import org.handwerkszeug.riak.op.RiakFuture;
import org.handwerkszeug.riak.op.RiakOperations;
import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.handwerkszeug.riak.op.SiblingHandler;
import org.handwerkszeug.riak.op.internal.DefaultRiakObjectResponse;
import org.handwerkszeug.riak.op.internal.NoOpResponse;
import org.handwerkszeug.riak.pbc.Riakclient.RpbContent;
import org.handwerkszeug.riak.pbc.Riakclient.RpbGetReq;
import org.handwerkszeug.riak.pbc.Riakclient.RpbGetResp;
import org.handwerkszeug.riak.pbc.Riakclient.RpbLink;
import org.handwerkszeug.riak.pbc.Riakclient.RpbPair;
import org.handwerkszeug.riak.util.NettyUtil;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

/**
 * @author taichi
 * @see <a href="http://wiki.basho.com/PBC-API.html">PBC API</a>
 * @see <a
 *      href="https://github.com/basho/riak_kv/blob/master/src/riak_kv_pb_socket.erl">Riak
 *      Protocol Buffers Server</a>
 */
public class PbcRiakOperations implements RiakOperations {

	static final Logger LOG = LoggerFactory.getLogger(PbcRiakOperations.class);

	final Channel channel;

	public PbcRiakOperations(Channel channel) {
		this.channel = channel;
	}

	@Override
	public RiakFuture listBuckets(RiakResponseHandler<List<String>> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture listKeys(String bucket, KeyHandler handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture getBucket(String bucket,
			RiakResponseHandler<Bucket> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture setBucket(Bucket bucket, RiakResponseHandler<_> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture get(final Location location,
			final RiakResponseHandler<RiakObject<byte[]>> handler) {
		RpbGetReq request = RpbGetReq.newBuilder()
				.setBucket(ByteString.copyFromUtf8(location.getBucket()))
				.setKey(ByteString.copyFromUtf8(location.getKey())).build();
		return handle("get", request, handler, new NettyUtil.MessageHandler() {
			@Override
			public boolean handle(Object receive) {
				if (receive instanceof RpbGetResp) {
					RpbGetResp resp = (RpbGetResp) receive;
					String vclock = "";
					if (resp.hasVclock()) {
						vclock = to(resp.getVclock());
					}
					int size = resp.getContentCount();
					if (size < 1) {
						LOG.error(Markers.BOUNDARY, Messages.NoContents,
								location);
						return true;
					} else if (1 < size) {
						LOG.warn(Markers.BOUNDARY, Messages.SiblingExists,
								vclock, size);
					}
					RiakObject<byte[]> ro = convert(location, vclock,
							resp.getContent(0));
					handler.handle(new DefaultRiakObjectResponse(ro));
					return true;
				}
				return false;
			}
		});
	}

	protected RiakObject<byte[]> convert(Location location, String vclock,
			RpbContent content) {
		DefaultRiakObject o = new DefaultRiakObject(location);
		o.setVectorClock(vclock);

		// TODO new array is created.
		o.setContent(content.getValue().toByteArray());
		if (content.hasContentType()) {
			o.setContentType(to(content.getContentType()));
		}
		if (content.hasCharset()) {
			o.setCharset(to(content.getCharset()));
		}
		if (content.hasContentEncoding()) {
			o.setContentEncoding(to(content.getContentEncoding()));
		}
		if (content.hasVtag()) {
			o.setVtag(to(content.getVtag()));
		}
		List<Link> list = new ArrayList<Link>(content.getLinksCount());
		o.setLinks(list);
		for (RpbLink pb : content.getLinksList()) {
			Location l = new Location(to(pb.getBucket()), to(pb.getKey()));
			Link link = new Link(l, to(pb.getTag()));
			list.add(link);
		}
		o.setLastModified(new Date(to(content.getLastMod())));
		o.setLastModifiedUsecs(new Date(to(content.getLastModUsecs())));
		Map<String, String> map = new HashMap<String, String>(
				content.getUsermetaCount());
		o.setUserMetadata(map);
		for (RpbPair pb : content.getUsermetaList()) {
			String key = to(pb.getKey());
			if ((key.isEmpty() == false) && pb.hasValue()) {
				map.put(key, to(pb.getValue()));
			}
		}
		return o;
	}

	static String to(ByteString bs) {
		if (bs == null) {
			return "";
		}
		return bs.toStringUtf8();
	}

	static long to(int uint32) {
		return uint32 & 0xFFFFFFFFL;
	}

	@Override
	public RiakFuture get(Location key, GetOptions options,
			RiakResponseHandler<RiakObject<byte[]>> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture get(Location key, GetOptions options,
			SiblingHandler siblingHandler,
			RiakResponseHandler<RiakObject<byte[]>> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture put(RiakObject<byte[]> content,
			RiakResponseHandler<List<RiakObject<byte[]>>> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture put(RiakObject<byte[]> content, PutOptions options,
			RiakResponseHandler<List<RiakObject<byte[]>>> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture delete(Location key, RiakResponseHandler<_> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture delete(Location key, Quorum quorum,
			RiakResponseHandler<_> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void mapReduce(MapReduceQueryConstructor constructor,
			MapReduceResponseHandler handler) {
		// TODO Auto-generated method stub

	}

	@Override
	public RiakFuture ping(final RiakResponseHandler<_> handler) {
		return handle("ping", MessageCodes.RpbPingReq, handler,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) {
						if (MessageCodes.RpbPingResp.equals(receive)) {
							handler.handle(new NoOpResponse() {
								@Override
								public String getMessage() {
									return "pong";
								};
							});
							return true;
						}
						return false;
					}
				});
	}

	@Override
	public RiakFuture getClientId(RiakResponseHandler<String> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture setClientId(String id, RiakResponseHandler<_> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture serverInfo(RiakResponseHandler<ServerInfo> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	protected <T> RiakFuture handle(final String name, Object send,
			final RiakResponseHandler<T> users,
			final NettyUtil.MessageHandler internal) {
		if (LOG.isDebugEnabled()) {
			LOG.debug(name);
		}
		ChannelPipeline pipeline = this.channel.getPipeline();
		pipeline.addLast(name, new SimpleChannelUpstreamHandler() {
			@Override
			public void messageReceived(ChannelHandlerContext ctx,
					MessageEvent e) throws Exception {
				ChannelPipeline pipeline = e.getChannel().getPipeline();
				pipeline.remove(name);
				Object o = e.getMessage();
				if (LOG.isDebugEnabled()) {
					LOG.debug(Markers.DETAIL, Messages.Receive, o);
				}
				if (o instanceof Riakclient.RpbErrorResp) {
					Riakclient.RpbErrorResp error = (Riakclient.RpbErrorResp) o;
					users.handle(new PbcErrorResponse<T>(error));
				} else {
					if (internal.handle(o) == false) {
						LOG.error(Markers.BOUNDARY, Messages.HaventProceed,
								name);
					}
				}
			}
		});
		try {
			ChannelFuture cf = this.channel.write(send);
			return new NettyUtil.FutureAdapter(cf);
		} catch (Exception e) {
			pipeline.remove(name);
			this.channel.close();
			throw new RiakException(e);
		}
	}
}
