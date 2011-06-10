package org.handwerkszeug.riak.transport.protobuf;

import static org.handwerkszeug.riak.util.Validation.notNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import net.iharder.Base64;

import org.handwerkszeug.riak.Markers;
import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.mapreduce.DefaultMapReduceQuery;
import org.handwerkszeug.riak.mapreduce.MapReduceQueryConstructor;
import org.handwerkszeug.riak.mapreduce.MapReduceResponse;
import org.handwerkszeug.riak.model.Bucket;
import org.handwerkszeug.riak.model.DefaultRiakObject;
import org.handwerkszeug.riak.model.GetOptions;
import org.handwerkszeug.riak.model.KeyResponse;
import org.handwerkszeug.riak.model.Link;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.PutOptions;
import org.handwerkszeug.riak.model.Quorum;
import org.handwerkszeug.riak.model.RiakFuture;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.model.RiakResponse;
import org.handwerkszeug.riak.model.ServerInfo;
import org.handwerkszeug.riak.nls.Messages;
import org.handwerkszeug.riak.op.Querying;
import org.handwerkszeug.riak.op.RiakOperations;
import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.handwerkszeug.riak.op.SiblingHandler;
import org.handwerkszeug.riak.transport.internal.CompletionSupport;
import org.handwerkszeug.riak.transport.internal.CountDownRiakFuture;
import org.handwerkszeug.riak.transport.internal.MessageHandler;
import org.handwerkszeug.riak.transport.protobuf.internal.MessageCodes;
import org.handwerkszeug.riak.transport.protobuf.internal.ProtoBufMapReduceResponse;
import org.handwerkszeug.riak.transport.protobuf.internal.RawProtoBufRiakclient;
import org.handwerkszeug.riak.transport.protobuf.internal.RawProtoBufRiakclient.RpbBucketProps;
import org.handwerkszeug.riak.transport.protobuf.internal.RawProtoBufRiakclient.RpbContent;
import org.handwerkszeug.riak.transport.protobuf.internal.RawProtoBufRiakclient.RpbDelReq;
import org.handwerkszeug.riak.transport.protobuf.internal.RawProtoBufRiakclient.RpbErrorResp;
import org.handwerkszeug.riak.transport.protobuf.internal.RawProtoBufRiakclient.RpbGetBucketReq;
import org.handwerkszeug.riak.transport.protobuf.internal.RawProtoBufRiakclient.RpbGetBucketResp;
import org.handwerkszeug.riak.transport.protobuf.internal.RawProtoBufRiakclient.RpbGetClientIdResp;
import org.handwerkszeug.riak.transport.protobuf.internal.RawProtoBufRiakclient.RpbGetReq;
import org.handwerkszeug.riak.transport.protobuf.internal.RawProtoBufRiakclient.RpbGetResp;
import org.handwerkszeug.riak.transport.protobuf.internal.RawProtoBufRiakclient.RpbGetServerInfoResp;
import org.handwerkszeug.riak.transport.protobuf.internal.RawProtoBufRiakclient.RpbLink;
import org.handwerkszeug.riak.transport.protobuf.internal.RawProtoBufRiakclient.RpbListBucketsResp;
import org.handwerkszeug.riak.transport.protobuf.internal.RawProtoBufRiakclient.RpbListKeysReq;
import org.handwerkszeug.riak.transport.protobuf.internal.RawProtoBufRiakclient.RpbListKeysResp;
import org.handwerkszeug.riak.transport.protobuf.internal.RawProtoBufRiakclient.RpbMapRedReq;
import org.handwerkszeug.riak.transport.protobuf.internal.RawProtoBufRiakclient.RpbMapRedResp;
import org.handwerkszeug.riak.transport.protobuf.internal.RawProtoBufRiakclient.RpbPair;
import org.handwerkszeug.riak.transport.protobuf.internal.RawProtoBufRiakclient.RpbPutReq;
import org.handwerkszeug.riak.transport.protobuf.internal.RawProtoBufRiakclient.RpbPutResp;
import org.handwerkszeug.riak.transport.protobuf.internal.RawProtoBufRiakclient.RpbSetBucketReq;
import org.handwerkszeug.riak.transport.protobuf.internal.RawProtoBufRiakclient.RpbSetClientIdReq;
import org.handwerkszeug.riak.util.HttpUtil;
import org.handwerkszeug.riak.util.StringUtil;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.ByteString.Output;

/**
 * @author taichi
 * @see <a href="http://wiki.basho.com/PBC-API.html">PBC API</a>
 * @see <a
 *      href="https://github.com/basho/riak_kv/blob/master/src/riak_kv_pb_socket.erl">Riak
 *      Protocol Buffers Server</a>
 */
public class ProtoBufRiakOperations implements RiakOperations {

	static final Logger LOG = LoggerFactory
			.getLogger(ProtoBufRiakOperations.class);

	protected CompletionSupport support;

	public ProtoBufRiakOperations(Channel channel) {
		this.support = new CompletionSupport(channel);
	}

	@Override
	public RiakFuture listBuckets(
			final RiakResponseHandler<List<String>> handler) {
		notNull(handler, "handler");

		final String procedure = "listBuckets";
		return handle(procedure, MessageCodes.RpbListBucketsReq, handler,
				new MessageHandler() {
					@Override
					public boolean handle(Object receive,
							CountDownRiakFuture future) throws Exception {
						if (receive instanceof RpbListBucketsResp) {
							RpbListBucketsResp resp = (RpbListBucketsResp) receive;
							List<String> list = new ArrayList<String>(resp
									.getBucketsCount());
							for (ByteString bs : resp.getBucketsList()) {
								list.add(to(bs));
							}
							handler.handle(ProtoBufRiakOperations.this.support
									.newResponse(list));
							future.setSuccess();
							return true;
						}
						return false;
					}
				});
	}

	@Override
	public RiakFuture listKeys(String bucket,
			final RiakResponseHandler<KeyResponse> handler) {
		notNull(bucket, "bucket");
		notNull(handler, "handler");

		RpbListKeysReq request = RpbListKeysReq.newBuilder()
				.setBucket(ByteString.copyFromUtf8(bucket)).build();

		final String procedure = "listKeys";
		return handle(procedure, request, handler, new MessageHandler() {
			@Override
			public boolean handle(Object receive, CountDownRiakFuture future)
					throws Exception {
				if (receive instanceof RpbListKeysResp) {
					RpbListKeysResp resp = (RpbListKeysResp) receive;
					boolean done = resp.getDone();
					List<String> list = new ArrayList<String>(resp
							.getKeysCount());
					for (ByteString bs : resp.getKeysList()) {
						list.add(to(bs));
					}
					KeyResponse kr = new KeyResponse(list, done);
					handler.handle(ProtoBufRiakOperations.this.support
							.newResponse(kr));
					if (done) {
						future.setSuccess();
					}
					return done;
				}
				return false;
			}
		});
	}

	@Override
	public RiakFuture getBucket(final String bucket,
			final RiakResponseHandler<Bucket> handler) {
		notNull(bucket, "bucket");
		notNull(handler, "handler");

		RpbGetBucketReq request = RpbGetBucketReq.newBuilder()
				.setBucket(ByteString.copyFromUtf8(bucket)).build();
		final String procedure = "getBucket";
		return handle(procedure, request, handler, new MessageHandler() {
			@Override
			public boolean handle(Object receive, CountDownRiakFuture future)
					throws Exception {
				if (receive instanceof RpbGetBucketResp) {
					RpbGetBucketResp resp = (RpbGetBucketResp) receive;
					RpbBucketProps props = resp.getProps();
					Bucket pb = new ProtoBufBucket(bucket);
					pb.setNumberOfReplicas(props.getNVal());
					pb.setAllowMulti(props.getAllowMult());
					handler.handle(ProtoBufRiakOperations.this.support
							.newResponse(pb));
					future.setSuccess();
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

		final String procedure = "setBucket";
		return handle(procedure, request, handler, new MessageHandler() {
			@Override
			public boolean handle(Object receive, CountDownRiakFuture future)
					throws Exception {
				if (MessageCodes.RpbSetBucketResp.equals(receive)) {
					handler.handle(ProtoBufRiakOperations.this.support
							.newResponse());
					future.setSuccess();
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
		return builder;
	}

	protected RiakFuture getSingle(RpbGetReq.Builder builder,
			final Location location,
			final RiakResponseHandler<RiakObject<byte[]>> handler) {
		return _get("get/single", builder, location, handler, new GetHandler() {
			@Override
			public void handle(RpbGetResp resp, String vclock) throws Exception {
				int size = resp.getContentCount();
				if (1 < size) {
					LOG.warn(Markers.BOUNDARY, Messages.SiblingExists, vclock,
							size);
				}
				RiakObject<byte[]> ro = convert(location, vclock,
						resp.getContent(0));
				handler.handle(ProtoBufRiakOperations.this.support
						.newResponse(ro));
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
					public void handle(RpbGetResp resp, String vclock)
							throws Exception {
						try {
							handler.begin();
							for (RpbContent c : resp.getContentList()) {
								RiakObject<byte[]> ro = convert(location,
										vclock, c);
								handler.handle(ProtoBufRiakOperations.this.support
										.newResponse(ro));
							}
						} finally {
							handler.end(ProtoBufRiakOperations.this.support
									.newResponse());
						}
					}
				});
	}

	interface GetHandler {
		void handle(RpbGetResp resp, String vclock) throws Exception;
	}

	protected RiakFuture _get(final String name, RpbGetReq.Builder builder,
			final Location location,
			final RiakResponseHandler<RiakObject<byte[]>> handler,
			final GetHandler getHandler) {
		RpbGetReq request = builder
				.setBucket(ByteString.copyFromUtf8(location.getBucket()))
				.setKey(ByteString.copyFromUtf8(location.getKey())).build();

		return handle(name, request, handler, new MessageHandler() {
			@Override
			public boolean handle(Object receive, CountDownRiakFuture future)
					throws Exception {
				if (receive instanceof RpbGetResp) {
					RpbGetResp resp = (RpbGetResp) receive;
					int size = resp.getContentCount();
					if (size < 1) {
						// in this case, REST API returns 404. PBC do so.
						handler.onError(new PbcErrorResponse(null) {
							@Override
							public String getMessage() {
								return String.format(Messages.NoContents,
										location);
							}

							@Override
							public int getResponseCode() {
								// a numeric code. Currently only
								// RIAKC_ERR_GENERAL=1 is defined.
								return 1;
							}
						});
						future.setFailure();
					} else {
						String vclock = toVclock(resp.getVclock());
						getHandler.handle(resp, vclock);
						future.setSuccess();
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
			LOG.debug(Markers.DETAIL, Messages.LastModified,
					HttpUtil.format(c.getTime()));
		}

		Map<String, String> map = new HashMap<String, String>(
				content.getUsermetaCount());
		o.setUserMetadata(map);
		for (RpbPair pb : content.getUsermetaList()) {
			String key = to(pb.getKey());
			if (key.isEmpty() == false && pb.hasValue()) {
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
	public RiakFuture post(RiakObject<byte[]> content,
			RiakResponseHandler<RiakObject<byte[]>> handler) {
		notNull(content, "content");
		notNull(handler, "handler");

		RpbPutReq.Builder builder = buildPostRequest(content);
		final String procedure = "post";
		return _post(procedure, content, handler, builder);
	}

	protected RiakFuture _post(final String procedure,
			RiakObject<byte[]> content,
			final RiakResponseHandler<RiakObject<byte[]>> handler,
			RpbPutReq.Builder builder) {
		final Location location = content.getLocation();
		final RiakObject<byte[]> copied = new DefaultRiakObject(content);
		return handle(procedure, builder.build(), handler,
				new MessageHandler() {
					@Override
					public boolean handle(Object receive,
							CountDownRiakFuture future) throws Exception {
						if (receive instanceof RpbPutResp) {
							RpbPutResp resp = (RpbPutResp) receive;
							Location newloc = location;
							if (resp.hasKey()) {
								newloc = new Location(location.getBucket(),
										resp.getKey().toStringUtf8());
							}
							copied.setLocation(newloc);

							if (resp.hasVclock()) {
								copied.setVectorClock(toVclock(resp.getVclock()));
							}
							handler.handle(ProtoBufRiakOperations.this.support
									.newResponse(copied));
							future.setSuccess();
							return true;
						}
						return false;
					}
				});
	}

	protected RpbPutReq.Builder buildPostRequest(RiakObject<byte[]> content) {
		RpbPutReq.Builder builder = RpbPutReq.newBuilder().setBucket(
				ByteString.copyFromUtf8(content.getLocation().getBucket()));
		builder.setContent(convert(content));
		return builder;
	}

	protected RpbPutReq.Builder buildPostRequest(RiakObject<byte[]> content,
			PutOptions options) {
		RpbPutReq.Builder builder = buildPostRequest(content);
		merge(options, builder);
		return builder;
	}

	@Override
	public RiakFuture post(RiakObject<byte[]> content, PutOptions options,
			final RiakResponseHandler<RiakObject<byte[]>> handler) {
		notNull(content, "content");
		notNull(options, "options");
		notNull(handler, "handler");

		RpbPutReq.Builder builder = buildPostRequest(content, options);
		if (options.getReturnBody() == false) {
			return _post("post/opt", content, handler, builder);
		}

		final Location location = content.getLocation();
		final String procedure = "post/returnbody";
		return handle(procedure, builder.build(), handler,
				new MessageHandler() {
					@Override
					public boolean handle(Object receive,
							CountDownRiakFuture future) throws Exception {
						if (receive instanceof RpbPutResp) {
							RpbPutResp resp = (RpbPutResp) receive;
							Location newloc = location;
							if (resp.hasKey()) {
								newloc = new Location(location.getBucket(),
										resp.getKey().toStringUtf8());
							}

							String vclock = toVclock(resp.getVclock());
							if (0 < resp.getContentCount()) {
								RpbContent c = resp.getContent(0);
								RiakObject<byte[]> ro = convert(newloc, vclock,
										c);
								handler.handle(ProtoBufRiakOperations.this.support
										.newResponse(ro));
							}
							future.setSuccess();
							return true;
						}
						return false;
					}
				});
	}

	@Override
	public RiakFuture put(RiakObject<byte[]> content,
			final RiakResponseHandler<_> handler) {
		notNull(content, "content");
		notNull(handler, "handler");

		RpbPutReq.Builder builder = buildPutRequest(content);
		final String procedure = "put";
		return handle(procedure, builder.build(), handler,
				new MessageHandler() {
					@Override
					public boolean handle(Object receive,
							CountDownRiakFuture future) throws Exception {
						if (receive instanceof RpbPutResp) {
							handler.handle(ProtoBufRiakOperations.this.support
									.newResponse());
							future.setSuccess();
							return true;
						}
						return false;
					}
				});
	}

	protected RpbPutReq.Builder buildPutRequest(RiakObject<byte[]> content) {
		Location loc = content.getLocation();
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
			final SiblingHandler handler) {
		notNull(content, "content");
		notNull(options, "options");
		notNull(handler, "handler");

		final Location location = content.getLocation();
		RpbPutReq.Builder builder = buildPutRequest(content, options);
		final String procedure = "put/opt";
		return handle(procedure, builder.build(), handler,
				new MessageHandler() {
					@Override
					public boolean handle(Object receive,
							CountDownRiakFuture future) throws Exception {
						if (receive instanceof RpbPutResp) {
							RpbPutResp resp = (RpbPutResp) receive;
							try {
								Location newloc = location;
								if (resp.hasKey()) {
									newloc = new Location(location.getBucket(),
											resp.getKey().toStringUtf8());
								}

								handler.begin();
								String vclock = toVclock(resp.getVclock());
								if (0 < resp.getContentCount()) {
									for (RpbContent c : resp.getContentList()) {
										RiakObject<byte[]> ro = convert(newloc,
												vclock, c);
										handler.handle(ProtoBufRiakOperations.this.support
												.newResponse(ro));
									}
								}
							} finally {
								handler.end(ProtoBufRiakOperations.this.support
										.newResponse());
							}
							future.setSuccess();
							return true;
						}
						return false;
					}
				});

	}

	protected RpbPutReq.Builder buildPutRequest(RiakObject<byte[]> content,
			PutOptions options) {
		RpbPutReq.Builder builder = buildPutRequest(content);
		merge(options, builder);
		return builder;
	}

	protected void merge(PutOptions options, RpbPutReq.Builder builder) {
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
		if (content.getLinks() != null && content.getLinks().isEmpty() == false) {
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

		if (content.getUserMetadata() != null
				&& content.getUserMetadata().isEmpty() == false) {
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

	protected RiakFuture _delete(final String name,
			final RiakResponseHandler<_> handler, RpbDelReq.Builder builder) {
		return handle(name, builder.build(), handler, new MessageHandler() {
			@Override
			public boolean handle(Object receive, CountDownRiakFuture future)
					throws Exception {
				if (MessageCodes.RpbDelResp.equals(receive)) {
					handler.handle(ProtoBufRiakOperations.this.support
							.newResponse());
					future.setSuccess();
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
	public RiakFuture delete(Location location, Quorum readWrite,
			RiakResponseHandler<_> handler) {
		notNull(location, "location");
		notNull(readWrite, "readWrite");
		notNull(handler, "handler");

		RpbDelReq.Builder builder = buildDeleteRequest(location);
		builder.setRw(readWrite.getInteger());
		return _delete("delete/quorum", handler, builder);
	}

	static final ByteString PbcJobEncoding = ByteString
			.copyFromUtf8(Querying.JobEncoding);

	@Override
	public RiakFuture mapReduce(MapReduceQueryConstructor constructor,
			RiakResponseHandler<MapReduceResponse> handler) {
		notNull(constructor, "constructor");
		notNull(handler, "handler");

		DefaultMapReduceQuery query = new DefaultMapReduceQuery();
		constructor.cunstruct(query);
		RpbMapRedReq.Builder builder = RpbMapRedReq.newBuilder();
		builder.setContentType(PbcJobEncoding);
		Output out = ByteString.newOutput();
		query.prepare(out);
		ByteString byteJson = out.toByteString();
		builder.setRequest(byteJson);

		return mapReduce(builder.build(), handler);
	}

	@Override
	public RiakFuture mapReduce(String rawJson,
			RiakResponseHandler<MapReduceResponse> handler) {
		notNull(rawJson, "rawJson");
		notNull(handler, "handler");

		RpbMapRedReq.Builder builder = RpbMapRedReq.newBuilder();
		builder.setContentType(PbcJobEncoding);
		builder.setRequest(ByteString.copyFromUtf8(rawJson));
		return mapReduce(builder.build(), handler);
	}

	protected RiakFuture mapReduce(RpbMapRedReq request,
			final RiakResponseHandler<MapReduceResponse> handler) {
		final String procedure = "mapReduce";
		return handle(procedure, request, handler, new MessageHandler() {
			@Override
			public boolean handle(Object receive, CountDownRiakFuture future)
					throws Exception {
				if (receive instanceof RpbMapRedResp) {
					RpbMapRedResp resp = (RpbMapRedResp) receive;
					MapReduceResponse response = new ProtoBufMapReduceResponse(
							resp);
					handler.handle(ProtoBufRiakOperations.this.support
							.newResponse(response));
					if (resp.getDone()) {
						future.setSuccess();
					}
					return resp.getDone();
				}
				return false;
			}
		});
	}

	@Override
	public RiakFuture ping(final RiakResponseHandler<String> handler) {
		final String procedure = "ping";
		return handle(procedure, MessageCodes.RpbPingReq, handler,
				new MessageHandler() {
					@Override
					public boolean handle(Object receive,
							CountDownRiakFuture future) throws Exception {
						if (MessageCodes.RpbPingResp.equals(receive)) {
							handler.handle(ProtoBufRiakOperations.this.support
									.newResponse("pong"));
							future.setSuccess();
							return true;
						}
						return false;
					}
				});
	}

	/**
	 * Get the client id used for this connection. Client ids are used for
	 * conflict resolution and each unique actor in the system should be
	 * assigned one. A client id is assigned randomly when the socket is
	 * connected and can be changed using SetClientId below.
	 * 
	 * @param handler
	 * @return
	 */
	public RiakFuture getClientId(final RiakResponseHandler<String> handler) {
		final String procedure = "getClientId";
		return handle(procedure, MessageCodes.RpbGetClientIdReq, handler,
				new MessageHandler() {
					@Override
					public boolean handle(Object receive,
							CountDownRiakFuture future) throws Exception {
						if (receive instanceof RpbGetClientIdResp) {
							RpbGetClientIdResp resp = (RpbGetClientIdResp) receive;
							String cid = to(resp.getClientId());
							handler.handle(ProtoBufRiakOperations.this.support
									.newResponse(cid));
							future.setSuccess();
							return true;
						}
						return false;
					}
				});
	}

	/**
	 * Set the client id for this connection. A library may want to set the
	 * client id if it has a good way to uniquely identify actors across
	 * reconnects. This will reduce vector clock bloat.
	 * 
	 * @param id
	 * @param handler
	 * @return
	 * @see <a
	 *      href="https://github.com/basho/riak_kv/blob/master/src/riak.erl">riak.erl</a>
	 */
	public RiakFuture setClientId(String id,
			final RiakResponseHandler<_> handler) {
		byte[] bytes = id.getBytes();
		if (4 < bytes.length) {
			throw new IllegalArgumentException(Messages.IllegalClientId);
		}
		RpbSetClientIdReq request = RpbSetClientIdReq.newBuilder()
				.setClientId(ByteString.copyFromUtf8(id)).build();
		final String procedure = "setClientId";
		return handle(procedure, request, handler, new MessageHandler() {
			@Override
			public boolean handle(Object receive, CountDownRiakFuture future)
					throws Exception {
				if (MessageCodes.RpbSetClientIdResp.equals(receive)) {
					handler.handle(ProtoBufRiakOperations.this.support
							.newResponse());
					future.setSuccess();
					return true;
				}
				return false;
			}
		});
	}

	/**
	 * get server information.
	 * 
	 * @param handler
	 * @return
	 */
	public RiakFuture getServerInfo(
			final RiakResponseHandler<ServerInfo> handler) {
		final String procedure = "getServerInfo";
		return handle(procedure, MessageCodes.RpbGetServerInfoReq, handler,
				new MessageHandler() {
					@Override
					public boolean handle(Object receive,
							CountDownRiakFuture future) throws Exception {
						if (receive instanceof RpbGetServerInfoResp) {
							RpbGetServerInfoResp resp = (RpbGetServerInfoResp) receive;
							ServerInfo info = new ServerInfo(
									to(resp.getNode()), to(resp
											.getServerVersion()));
							handler.handle(ProtoBufRiakOperations.this.support
									.newResponse(info));
							future.setSuccess();
							return true;
						}
						return false;
					}
				});
	}

	protected <T> RiakFuture handle(final String name, Object send,
			final RiakResponseHandler<T> users, final MessageHandler internal) {
		return this.support.handle(name, send, users, new MessageHandler() {
			@Override
			public boolean handle(Object receive, CountDownRiakFuture future)
					throws Exception {
				if (receive instanceof RpbErrorResp) {
					RpbErrorResp error = (RpbErrorResp) receive;
					users.onError(new PbcErrorResponse(error));
					future.setFailure();
					return true;
				} else {
					return internal.handle(receive, future);
				}
			}
		});
	}

	class PbcErrorResponse implements RiakResponse {

		final RawProtoBufRiakclient.RpbErrorResp error;

		public PbcErrorResponse(RawProtoBufRiakclient.RpbErrorResp error) {
			this.error = error;
		}

		@Override
		public int getResponseCode() {
			return this.error.getErrcode();
		}

		@Override
		public String getMessage() {
			return this.error.getErrmsg().toStringUtf8();
		}

		@Override
		public void operationComplete() {
			ProtoBufRiakOperations.this.support.complete();
		}
	}

}
