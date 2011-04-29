package org.handwerkszeug.riak.pbc;

import static org.handwerkszeug.riak.util.Validation.notNull;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import net.iharder.Base64;

import org.handwerkszeug.riak.Markers;
import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.mapreduce.MapReduceQueryConstructor;
import org.handwerkszeug.riak.mapreduce.MapReduceResponseHandler;
import org.handwerkszeug.riak.model.Bucket;
import org.handwerkszeug.riak.model.DefaultRiakObject;
import org.handwerkszeug.riak.model.GetOptions;
import org.handwerkszeug.riak.model.Link;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.PutOptions;
import org.handwerkszeug.riak.model.Quorum;
import org.handwerkszeug.riak.model.RiakFuture;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.model.ServerInfo;
import org.handwerkszeug.riak.model.internal.AbstractRiakResponse;
import org.handwerkszeug.riak.model.internal.DefaultRiakObjectResponse;
import org.handwerkszeug.riak.model.internal.NoContents;
import org.handwerkszeug.riak.model.internal.NoOpResponse;
import org.handwerkszeug.riak.nls.Messages;
import org.handwerkszeug.riak.op.KeyHandler;
import org.handwerkszeug.riak.op.RiakOperations;
import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.handwerkszeug.riak.op.SiblingHandler;
import org.handwerkszeug.riak.pbc.Riakclient.RpbBucketProps;
import org.handwerkszeug.riak.pbc.Riakclient.RpbContent;
import org.handwerkszeug.riak.pbc.Riakclient.RpbDelReq;
import org.handwerkszeug.riak.pbc.Riakclient.RpbErrorResp;
import org.handwerkszeug.riak.pbc.Riakclient.RpbGetBucketReq;
import org.handwerkszeug.riak.pbc.Riakclient.RpbGetBucketResp;
import org.handwerkszeug.riak.pbc.Riakclient.RpbGetClientIdResp;
import org.handwerkszeug.riak.pbc.Riakclient.RpbGetReq;
import org.handwerkszeug.riak.pbc.Riakclient.RpbGetResp;
import org.handwerkszeug.riak.pbc.Riakclient.RpbGetServerInfoResp;
import org.handwerkszeug.riak.pbc.Riakclient.RpbLink;
import org.handwerkszeug.riak.pbc.Riakclient.RpbListBucketsResp;
import org.handwerkszeug.riak.pbc.Riakclient.RpbListKeysReq;
import org.handwerkszeug.riak.pbc.Riakclient.RpbListKeysResp;
import org.handwerkszeug.riak.pbc.Riakclient.RpbPair;
import org.handwerkszeug.riak.pbc.Riakclient.RpbPutReq;
import org.handwerkszeug.riak.pbc.Riakclient.RpbPutResp;
import org.handwerkszeug.riak.pbc.Riakclient.RpbSetBucketReq;
import org.handwerkszeug.riak.pbc.Riakclient.RpbSetClientIdReq;
import org.handwerkszeug.riak.util.NettyUtil;
import org.handwerkszeug.riak.util.StringUtil;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.MessageEvent;
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
	public RiakFuture listBuckets(
			final RiakResponseHandler<List<String>> handler) {
		notNull(handler, "handler");

		return handle("listBuckets", MessageCodes.RpbListBucketsReq, handler,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) {
						if (receive instanceof RpbListBucketsResp) {
							RpbListBucketsResp resp = (RpbListBucketsResp) receive;
							final List<String> list = new ArrayList<String>(
									resp.getBucketsCount());
							for (ByteString bs : resp.getBucketsList()) {
								list.add(to(bs));
							}
							handler.handle(new AbstractRiakResponse<List<String>>() {
								@Override
								public List<String> getResponse() {
									return list;
								}
							});
							return true;
						}
						return false;
					}
				});
	}

	@Override
	public RiakFuture listKeys(String bucket, final KeyHandler handler) {
		notNull(bucket, "bucket");
		notNull(handler, "handler");

		RpbListKeysReq request = RpbListKeysReq.newBuilder()
				.setBucket(ByteString.copyFromUtf8(bucket)).build();
		final String name = "listKeys";
		if (LOG.isDebugEnabled()) {
			LOG.debug(name);
		}
		ChannelPipeline pipeline = this.channel.getPipeline();
		pipeline.addLast(name, new NettyUtil.UpstreamHandler<_>(LOG, handler) {

			@Override
			public void messageReceived(ChannelHandlerContext ctx,
					MessageEvent e) throws Exception {
				ChannelPipeline pipeline = e.getChannel().getPipeline();
				Object o = e.getMessage();
				if (LOG.isDebugEnabled()) {
					LOG.debug(Markers.DETAIL, Messages.Receive, name, o);
				}
				if (o instanceof RpbErrorResp) {
					RpbErrorResp error = (RpbErrorResp) o;
					handler.handle(new PbcErrorResponse<_>(error));
				} else if (o instanceof RpbListKeysResp) {
					RpbListKeysResp resp = (RpbListKeysResp) o;
					boolean done = resp.getDone();
					if (done) {
						pipeline.remove(name);
					}
					final List<String> list = new ArrayList<String>(resp
							.getKeysCount());
					for (ByteString bs : resp.getKeysList()) {
						list.add(to(bs));
					}
					handler.handleKeys(
							new AbstractRiakResponse<List<String>>() {
								@Override
								public List<String> getResponse() {
									return list;
								}
							}, done);

				} else {
					// do nothing.
					ctx.sendUpstream(e);
				}
			}
		});
		try {
			ChannelFuture cf = this.channel.write(request);
			return new NettyUtil.FutureAdapter(cf);
		} catch (Exception e) {
			pipeline.remove(name);
			this.channel.close();
			throw new RiakException(e);
		}
	}

	@Override
	public RiakFuture getBucket(final String bucket,
			final RiakResponseHandler<Bucket> handler) {
		notNull(bucket, "bucket");
		notNull(handler, "handler");

		RpbGetBucketReq request = RpbGetBucketReq.newBuilder()
				.setBucket(ByteString.copyFromUtf8(bucket)).build();
		return handle("getBucket", request, handler,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) {
						if (receive instanceof RpbGetBucketResp) {
							RpbGetBucketResp resp = (RpbGetBucketResp) receive;
							RpbBucketProps props = resp.getProps();
							final PbcBucket pb = new PbcBucket(bucket);
							pb.setNumberOfReplicas(props.getNVal());
							pb.setAllowMulti(props.getAllowMult());
							handler.handle(new AbstractRiakResponse<Bucket>() {
								@Override
								public Bucket getResponse() {
									return pb;
								}
							});
							return true;
						}
						return false;
					}
				});
	}

	@Override
	public RiakFuture setBucket(Bucket bucket,
			final RiakResponseHandler<_> handler) {
		notNull(bucket, "bucket");
		notNull(handler, "handler");

		RpbBucketProps props = RpbBucketProps.newBuilder()
				.setNVal(bucket.getNumberOfReplicas())
				.setAllowMult(bucket.getAllowMulti()).build();
		RpbSetBucketReq request = RpbSetBucketReq.newBuilder()
				.setBucket(ByteString.copyFromUtf8(bucket.getName()))
				.setProps(props).build();
		return handle("setBucket", request, handler,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) {
						if (MessageCodes.RpbSetBucketResp.equals(receive)) {
							handler.handle(new NoOpResponse());
							return true;
						}
						return false;
					}
				});
	}

	@Override
	public RiakFuture get(final Location location,
			RiakResponseHandler<RiakObject<byte[]>> handler) {
		notNull(location, "location");
		notNull(handler, "handler");

		return getSingle(RpbGetReq.newBuilder(), location, handler);
	}

	@Override
	public RiakFuture get(Location location, GetOptions options,
			RiakResponseHandler<RiakObject<byte[]>> handler) {
		notNull(location, "location");
		notNull(options, "options");
		notNull(handler, "handler");

		return getSingle(from(options), location, handler);
	}

	protected RpbGetReq.Builder from(GetOptions options) {
		RpbGetReq.Builder builder = RpbGetReq.newBuilder();
		if (options.getReadQuorum() != null) {
			builder.setR(options.getReadQuorum().getInteger());
		}
		// TODO PR support.
		// TODO emulate other options.
		return builder;
	}

	protected RiakFuture getSingle(RpbGetReq.Builder builder,
			final Location location,
			final RiakResponseHandler<RiakObject<byte[]>> handler) {
		return _get("get/single", builder, location, handler, new GetHandler() {
			@Override
			public void handle(RpbGetResp resp, String vclock) {
				int size = resp.getContentCount();
				if (1 < size) {
					LOG.warn(Markers.BOUNDARY, Messages.SiblingExists, vclock,
							size);
				}
				RiakObject<byte[]> ro = convert(location, vclock,
						resp.getContent(0));
				handler.handle(new DefaultRiakObjectResponse(ro));
			}
		});
	}

	@Override
	public RiakFuture get(final Location location, GetOptions options,
			final SiblingHandler handler) {
		notNull(location, "location");
		notNull(options, "options");
		notNull(handler, "handler");

		RpbGetReq.Builder builder = from(options);
		return _get("get/sibling", builder, location, handler,
				new GetHandler() {
					@Override
					public void handle(RpbGetResp resp, String vclock) {
						try {
							handler.begin();
							for (RpbContent c : resp.getContentList()) {
								RiakObject<byte[]> ro = convert(location,
										vclock, c);
								handler.handle(new DefaultRiakObjectResponse(ro));
							}
						} finally {
							handler.end();
						}
					}
				});
	}

	interface GetHandler {
		void handle(RpbGetResp resp, String vclock);
	}

	protected RiakFuture _get(String name, RpbGetReq.Builder builder,
			final Location location,
			final RiakResponseHandler<RiakObject<byte[]>> handler,
			final GetHandler getHandler) {
		RpbGetReq request = builder
				.setBucket(ByteString.copyFromUtf8(location.getBucket()))
				.setKey(ByteString.copyFromUtf8(location.getKey())).build();
		return handle(name, request, handler, new NettyUtil.MessageHandler() {
			@Override
			public boolean handle(Object receive) {
				if (receive instanceof RpbGetResp) {
					RpbGetResp resp = (RpbGetResp) receive;
					int size = resp.getContentCount();
					if (size < 1) {
						handler.handle(new NoContents<RiakObject<byte[]>>(
								location));
					} else {
						String vclock = toVclock(resp.getVclock());
						getHandler.handle(resp, vclock);
					}
					return true;
				}
				return false;
			}
		});
	}

	public String toVclock(ByteString clock) {
		if (clock != null) {
			return Base64.encodeBytes(clock.toByteArray());
		}
		return null;
	}

	public ByteString fromVclock(String base64) {
		if (StringUtil.isEmpty(base64) == false) {
			try {
				byte[] bytes = Base64.decode(base64);
				return ByteString.copyFrom(bytes);
			} catch (IOException e) {
				throw new RiakException(e);
			}
		}
		return null;
	}

	protected RiakObject<byte[]> convert(Location location, String vclock,
			RpbContent content) {
		DefaultRiakObject o = new DefaultRiakObject(location);
		o.setVectorClock(vclock);

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

		// see.
		// https://github.com/basho/riak-erlang-client/blob/master/src/riakc_pb.erl
		// https://github.com/basho/riak_kv/blob/master/src/riak_kv_put_fsm.erl
		// http://www.erlang.org/doc/man/erlang.html#now-0
		long milis = TimeUnit.SECONDS.toMillis(to(content.getLastMod()));
		milis += TimeUnit.MICROSECONDS.toMillis(to(content.getLastModUsecs()));
		Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		c.setTimeInMillis(milis);
		o.setLastModified(c.getTime());

		if (LOG.isDebugEnabled()) {
			SimpleDateFormat fmt = new SimpleDateFormat(
					"EEE, d MMM yyyy HH:mm:ss z", Locale.ENGLISH);
			fmt.setCalendar(c);
			LOG.debug(Markers.DETAIL, Messages.LastModified,
					fmt.format(c.getTime()));
		}

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
	public RiakFuture put(RiakObject<byte[]> content,
			final RiakResponseHandler<List<RiakObject<byte[]>>> handler) {
		notNull(content, "content");
		notNull(handler, "handler");

		Location loc = content.getLocation();
		RpbPutReq.Builder builder = buildPutRequest(content, loc);
		return handle("put", builder.build(), handler,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) {
						if (receive instanceof RpbPutResp) {
							handler.handle(new AbstractRiakResponse<List<RiakObject<byte[]>>>() {
								@Override
								public List<RiakObject<byte[]>> getResponse() {
									return Collections.emptyList();
								}
							});
							return true;
						}
						return false;
					}
				});
	}

	protected RpbPutReq.Builder buildPutRequest(RiakObject<byte[]> content,
			Location loc) {
		RpbPutReq.Builder builder = RpbPutReq.newBuilder()
				.setBucket(ByteString.copyFromUtf8(loc.getBucket()))
				.setKey(ByteString.copyFromUtf8(loc.getKey()));
		String vclock = content.getVectorClock();
		if (StringUtil.isEmpty(vclock) == false) {
			builder.setVclock(fromVclock(vclock));
		}
		builder.setContent(convert(content));
		return builder;
	}

	@Override
	public RiakFuture put(RiakObject<byte[]> content, PutOptions options,
			final RiakResponseHandler<List<RiakObject<byte[]>>> handler) {
		notNull(content, "content");
		notNull(options, "options");
		notNull(handler, "handler");

		final Location location = content.getLocation();
		RpbPutReq.Builder builder = buildPutRequest(content, location, options);
		return handle("put/opt", builder.build(), handler,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) {
						if (receive instanceof RpbPutResp) {
							RpbPutResp resp = (RpbPutResp) receive;
							final List<RiakObject<byte[]>> list = new ArrayList<RiakObject<byte[]>>(
									resp.getContentCount());
							if (0 < resp.getContentCount()) {
								String vclock = toVclock(resp.getVclock());
								for (RpbContent c : resp.getContentList()) {
									RiakObject<byte[]> ro = convert(location,
											vclock, c);
									list.add(ro);
								}
							}
							handler.handle(new AbstractRiakResponse<List<RiakObject<byte[]>>>() {
								@Override
								public List<RiakObject<byte[]>> getResponse() {
									return list;
								}
							});
							return true;
						}
						return false;
					}
				});

	}

	protected RpbPutReq.Builder buildPutRequest(RiakObject<byte[]> content,
			Location loc, PutOptions options) {
		RpbPutReq.Builder builder = buildPutRequest(content, loc);
		if (StringUtil.isEmpty(options.getVectorClock()) == false) {
			ByteString clock = fromVclock(options.getVectorClock());
			builder.setVclock(clock);
		}
		if (options.getWriteQuorum() != null) {
			builder.setW(options.getWriteQuorum().getInteger());
		}
		if (options.getDurableWriteQuorum() != null) {
			builder.setDw(options.getDurableWriteQuorum().getInteger());
		}
		if (options.getReturnBody()) {
			builder.setReturnBody(options.getReturnBody());
		}
		return builder;
	}

	protected RpbContent convert(RiakObject<byte[]> content) {
		RpbContent.Builder builder = RpbContent.newBuilder();
		builder.setValue(ByteString.copyFrom(content.getContent()));
		if (StringUtil.isEmpty(content.getContentType()) == false) {
			builder.setContentType(ByteString.copyFromUtf8(content
					.getContentType()));
		}
		if (StringUtil.isEmpty(content.getCharset()) == false) {
			builder.setCharset(ByteString.copyFromUtf8(content.getCharset()));
		}
		if (StringUtil.isEmpty(content.getContentEncoding()) == false) {
			builder.setContentEncoding(ByteString.copyFromUtf8(content
					.getContentEncoding()));
		}
		if (StringUtil.isEmpty(content.getVtag()) == false) {
			builder.setVtag(ByteString.copyFromUtf8(content.getVtag()));
		}
		if ((content.getLinks() != null)
				&& (content.getLinks().isEmpty() == false)) {
			for (Link link : content.getLinks()) {
				RpbLink.Builder lb = RpbLink.newBuilder();
				Location loc = link.getLocation();
				if (StringUtil.isEmpty(loc.getBucket()) == false) {
					lb.setBucket(ByteString.copyFromUtf8(loc.getBucket()));
				}
				if (StringUtil.isEmpty(loc.getKey()) == false) {
					lb.setKey(ByteString.copyFromUtf8(loc.getKey()));
				}
				if (StringUtil.isEmpty(link.getTag()) == false) {
					lb.setTag(ByteString.copyFromUtf8(link.getTag()));
				}
				builder.addLinks(lb.build());
			}
		}

		if (content.getLastModified() != null) {
			Date lm = content.getLastModified();
			long mili = lm.getTime();
			int sec = (int) (mili / 1000);
			int msec = (int) (mili % 1000);
			builder.setLastMod(sec);
			builder.setLastModUsecs(msec);
		}

		if ((content.getUserMetadata() != null)
				&& (content.getUserMetadata().isEmpty() == false)) {
			Map<String, String> map = content.getUserMetadata();
			for (String key : map.keySet()) {
				RpbPair.Builder b = RpbPair.newBuilder();
				b.setKey(ByteString.copyFromUtf8(key));
				String v = map.get(key);
				if (StringUtil.isEmpty(v) == false) {
					b.setValue(ByteString.copyFromUtf8(v));
				}
				builder.addUsermeta(b.build());
			}
		}

		return builder.build();
	}

	@Override
	public RiakFuture delete(Location location,
			final RiakResponseHandler<_> handler) {
		notNull(location, "location");
		notNull(handler, "handler");

		RpbDelReq.Builder builder = buildDeleteRequest(location);
		return _delete("delete", handler, builder);
	}

	protected RiakFuture _delete(String name,
			final RiakResponseHandler<_> handler, RpbDelReq.Builder builder) {
		return handle(name, builder.build(), handler,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) {
						if (MessageCodes.RpbDelResp.equals(receive)) {
							handler.handle(new NoOpResponse());
							return true;
						}
						return false;
					}
				});
	}

	protected RpbDelReq.Builder buildDeleteRequest(Location location) {
		return RpbDelReq.newBuilder()
				.setBucket(ByteString.copyFromUtf8(location.getBucket()))
				.setKey(ByteString.copyFromUtf8(location.getKey()));
	}

	@Override
	public RiakFuture delete(Location location, Quorum quorum,
			RiakResponseHandler<_> handler) {
		notNull(location, "location");
		notNull(quorum, "quorum");
		notNull(handler, "handler");

		RpbDelReq.Builder builder = buildDeleteRequest(location);
		builder.setRw(quorum.getInteger());
		return _delete("delete/quorum", handler, builder);
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
	public RiakFuture getClientId(final RiakResponseHandler<String> handler) {
		return handle("getClientId", MessageCodes.RpbGetClientIdReq, handler,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) {
						if (receive instanceof RpbGetClientIdResp) {
							RpbGetClientIdResp resp = (RpbGetClientIdResp) receive;
							final String cid = to(resp.getClientId());
							handler.handle(new AbstractRiakResponse<String>() {
								@Override
								public String getResponse() {
									return cid;
								}
							});
							return true;
						}
						return false;
					}
				});
	}

	@Override
	/**
	 * @see <a href="https://github.com/basho/riak_kv/blob/master/src/riak.erl">riak.erl</a>
	 */
	public RiakFuture setClientId(String id,
			final RiakResponseHandler<_> handler) {
		byte[] bytes = id.getBytes();
		if (4 < bytes.length) {
			throw new IllegalArgumentException(Messages.IllegalClientId);
		}
		RpbSetClientIdReq request = RpbSetClientIdReq.newBuilder()
				.setClientId(ByteString.copyFromUtf8(id)).build();
		return handle("setClientId", request, handler,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) {
						if (MessageCodes.RpbSetClientIdResp.equals(receive)) {
							handler.handle(new NoOpResponse());
							return true;
						}
						return false;
					}
				});
	}

	@Override
	public RiakFuture getServerInfo(
			final RiakResponseHandler<ServerInfo> handler) {
		return handle("getServerInfo", MessageCodes.RpbGetServerInfoReq,
				handler, new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) {
						if (receive instanceof RpbGetServerInfoResp) {
							RpbGetServerInfoResp resp = (RpbGetServerInfoResp) receive;
							final ServerInfo info = new ServerInfo(to(resp
									.getNode()), to(resp.getServerVersion()));
							handler.handle(new AbstractRiakResponse<ServerInfo>() {
								@Override
								public ServerInfo getResponse() {
									return info;
								}
							});
							return true;
						}
						return false;
					}
				});
	}

	protected <T> RiakFuture handle(final String name, Object send,
			final RiakResponseHandler<T> users,
			final NettyUtil.MessageHandler internal) {
		if (LOG.isDebugEnabled()) {
			LOG.debug(name);
		}
		ChannelPipeline pipeline = this.channel.getPipeline();
		pipeline.addLast(name, new NettyUtil.UpstreamHandler<T>(LOG, users) {
			@Override
			public void messageReceived(ChannelHandlerContext ctx,
					MessageEvent e) throws Exception {
				ChannelPipeline pipeline = e.getChannel().getPipeline();
				pipeline.remove(name);
				Object o = e.getMessage();
				if (LOG.isDebugEnabled()) {
					LOG.debug(Markers.DETAIL, Messages.Receive, name, o);
				}
				if (o instanceof RpbErrorResp) {
					RpbErrorResp error = (RpbErrorResp) o;
					users.handle(new PbcErrorResponse<T>(error));
				} else {
					if (internal.handle(o) == false) {
						LOG.error(Markers.BOUNDARY, Messages.HaventProceed,
								name);
						ctx.sendUpstream(e);
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
