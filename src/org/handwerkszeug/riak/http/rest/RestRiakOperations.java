package org.handwerkszeug.riak.http.rest;

import static org.handwerkszeug.riak.util.Validation.notNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.Markers;
import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.http.HttpRiakOperations;
import org.handwerkszeug.riak.http.InputStreamHandler;
import org.handwerkszeug.riak.http.LinkCondition;
import org.handwerkszeug.riak.http.RiakHttpHeaders;
import org.handwerkszeug.riak.http.StreamResponseHandler;
import org.handwerkszeug.riak.http.rest.internal.BucketHolder;
import org.handwerkszeug.riak.http.rest.internal.ContinuousMessageHandler;
import org.handwerkszeug.riak.http.rest.internal.RequestFactory;
import org.handwerkszeug.riak.http.rest.internal.SimpleMessageHandler;
import org.handwerkszeug.riak.mapreduce.DefaultMapReduceQuery;
import org.handwerkszeug.riak.mapreduce.MapReduceQueryConstructor;
import org.handwerkszeug.riak.mapreduce.MapReduceResponse;
import org.handwerkszeug.riak.model.Bucket;
import org.handwerkszeug.riak.model.DefaultRiakObject;
import org.handwerkszeug.riak.model.GetOptions;
import org.handwerkszeug.riak.model.KeyResponse;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.PutOptions;
import org.handwerkszeug.riak.model.Quorum;
import org.handwerkszeug.riak.model.Range;
import org.handwerkszeug.riak.model.RiakFuture;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.model.internal.AbstractRiakObject;
import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.handwerkszeug.riak.op.SiblingHandler;
import org.handwerkszeug.riak.op.internal.CompletionSupport;
import org.handwerkszeug.riak.util.JsonUtil;
import org.handwerkszeug.riak.util.NettyUtil;
import org.handwerkszeug.riak.util.StringUtil;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMessage;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.MultipartResponseDecoder;
import org.jboss.netty.handler.codec.http.PartMessage;
import org.jboss.netty.handler.stream.ChunkedStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author taichi
 */
public class RestRiakOperations implements HttpRiakOperations {

	static final Logger LOG = LoggerFactory.getLogger(RestRiakOperations.class);

	RestConfig config;
	// for luwak support.
	Channel channel;
	CompletionSupport support;
	RequestFactory factory;

	public RestRiakOperations(String host, RestConfig config, Channel channel) {
		this(host, config, channel, new RequestFactory(host, config));
	}

	public RestRiakOperations(String host, RestConfig config, Channel channel,
			RequestFactory factory) {
		notNull(host, "host");
		notNull(channel, "channel");
		this.channel = channel;
		this.support = new CompletionSupport(channel);
		this.config = config;
		this.factory = factory;

	}

	@Override
	public RiakFuture ping(final RiakResponseHandler<String> handler) {
		notNull(handler, "handler");

		HttpRequest request = this.factory.newPingRequest();
		final String procedure = "ping";
		return handle(procedure, request, handler,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) throws Exception {
						if (receive instanceof HttpResponse) {
							HttpResponse response = (HttpResponse) receive;
							if (NettyUtil.isSuccessful(response.getStatus())) {
								handler.handle(RestRiakOperations.this.support
										.newResponse("pong"));
								return true;
							}
						}
						return false;
					}
				});
	}

	@Override
	public void setClientId(String clientId) {
		this.factory.setClientId(clientId);
	}

	@Override
	public String getClientId() {
		return this.factory.getClientId();
	}

	@Override
	public RiakFuture listBuckets(
			final RiakResponseHandler<List<String>> handler) {
		notNull(handler, "handler");

		HttpRequest request = this.factory.newListBucketsRequest();
		final String procedure = "listBuckets";
		return handle(procedure, request, handler,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) throws Exception {
						if (receive instanceof HttpResponse) {
							HttpResponse response = (HttpResponse) receive;
							ChannelBuffer buffer = response.getContent();
							ObjectNode node = to(buffer);
							if (node != null) {
								List<String> list = JsonUtil.to(node
										.get("buckets"));
								handler.handle(RestRiakOperations.this.support
										.newResponse(list));
								return true;
							}
						}
						return false;
					}
				});
	}

	@SuppressWarnings("unchecked")
	<T extends JsonNode> T to(ChannelBuffer buffer, T... t) {
		try {
			if (buffer != null && buffer.readable()) {
				ObjectMapper objectMapper = new ObjectMapper();
				JsonNode node = objectMapper
						.readTree(new ChannelBufferInputStream(buffer));
				Class<?> clazz = t.getClass().getComponentType();
				if (clazz.isAssignableFrom(node.getClass())) {
					return (T) node;
				}
			}
		} catch (IOException e) {
			LOG.error(Markers.BOUNDARY, e.getMessage(), e);
			throw new RiakException(e);
		}
		return null;
	}

	@Override
	public RiakFuture listKeys(String bucket,
			final RiakResponseHandler<KeyResponse> handler) {
		notNull(bucket, "bucket");
		notNull(handler, "handler");

		HttpRequest request = this.factory.newListKeysRequest(bucket);
		final String procedure = "listKeys";
		return handle(procedure, request, handler,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) throws Exception {
						if (receive instanceof HttpResponse) {
							HttpResponse response = (HttpResponse) receive;
							if (NettyUtil.isSuccessful(response.getStatus())) {
								boolean done = response.isChunked() == false;
								if (done) {
									_listKeys(response.getContent(), handler);
								}
								return done;
							}
						} else if (receive instanceof HttpChunk) {
							HttpChunk chunk = (HttpChunk) receive;
							boolean done = chunk.isLast();
							if (done == false) {
								_listKeys(chunk.getContent(), handler);
							}
							return done;
						}
						return false;
					}
				});
	}

	protected void _listKeys(ChannelBuffer buffer,
			final RiakResponseHandler<KeyResponse> handler) throws Exception {
		ObjectNode on = to(buffer);
		if (on != null) {
			JsonNode node = on.get("keys");
			if (node != null) {
				List<String> list = JsonUtil.to(node);
				KeyResponse kr = new KeyResponse(list, list.isEmpty());
				handler.handle(this.support.newResponse(kr));
			}
		}
	}

	@Override
	public RiakFuture getBucket(String bucket,
			final RiakResponseHandler<Bucket> handler) {
		notNull(bucket, "bucket");
		notNull(handler, "handler");

		HttpRequest request = this.factory.newGetBucketRequest(bucket);

		final String procedure = "getBucket";
		return handle(procedure, request, handler,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) throws Exception {
						if (receive instanceof HttpResponse) {
							HttpResponse response = (HttpResponse) receive;
							if (NettyUtil.isSuccessful(response.getStatus())) {
								ObjectMapper objectMapper = new ObjectMapper();
								BucketHolder holder = objectMapper.readValue(
										new ChannelBufferInputStream(response
												.getContent()),
										BucketHolder.class);
								handler.handle(RestRiakOperations.this.support
										.newResponse(holder.props));
								return true;
							}
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

		HttpRequest request = this.factory.newSetBucketRequest(bucket);
		final String procedure = "setBucket";
		return handle(procedure, request, handler);
	}

	@Override
	public RiakFuture get(Location location,
			RiakResponseHandler<RiakObject<byte[]>> handler) {
		notNull(location, "location");
		notNull(handler, "handler");

		return getSingle(this.factory.newGetRequst(location), location, handler);
	}

	@Override
	public RiakFuture get(Location location, GetOptions options,
			RiakResponseHandler<RiakObject<byte[]>> handler) {
		notNull(location, "location");
		notNull(options, "options");
		notNull(handler, "handler");

		return getSingle(this.factory.newGetRequst(location, options),
				location, handler);
	}

	protected RiakFuture getSingle(HttpRequest request,
			final Location location,
			final RiakResponseHandler<RiakObject<byte[]>> handler) {

		String procedure = "get/single";
		return handle(procedure, request, handler,
				new NettyUtil.ChunkedMessageAggregator(procedure,
						new NettyUtil.ChunkedMessageHandler() {
							@Override
							public void handle(HttpResponse response,
									ChannelBuffer buffer) throws Exception {
								RiakObject<byte[]> ro = RestRiakOperations.this.factory
										.convert(response, buffer, location);
								handler.handle(RestRiakOperations.this.support
										.newResponse(ro));
							}
						}));
	}

	@Override
	public RiakFuture get(final Location location, GetOptions options,
			final SiblingHandler handler) {
		notNull(location, "location");
		notNull(options, "options");
		notNull(handler, "handler");

		HttpRequest request = this.factory.newGetRequst(location, options);
		request.setHeader(HttpHeaders.Names.ACCEPT, RiakHttpHeaders.MULTI_PART);

		final String procedure = "get/sibling";
		return handle(procedure, request, handler,
				new NettyUtil.MessageHandler() {
					String vclock;

					@Override
					public boolean handle(Object receive) throws Exception {
						if (receive instanceof HttpResponse) {
							HttpResponse response = (HttpResponse) receive;
							this.vclock = response
									.getHeader(RiakHttpHeaders.VECTOR_CLOCK);
							handler.begin();
							return false;
						} else if (receive instanceof PartMessage) {
							PartMessage part = (PartMessage) receive;
							boolean done = part.isLast();
							part.setHeader(RiakHttpHeaders.VECTOR_CLOCK,
									this.vclock);

							if (done) {
								handler.end(RestRiakOperations.this.support
										.newResponse());
							} else {
								RiakObject<byte[]> ro = RestRiakOperations.this.factory
										.convert(part, part.getContent(),
												location);
								handler.handle(RestRiakOperations.this.support
										.newResponse(ro));
							}
							return done;
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

		HttpRequest request = this.factory.newPutRequest(content);

		final String procedure = "put";
		return handle(procedure, request, handler);
	}

	/**
	 * if returning body has sibling then call get with sibling call
	 * automatically.
	 */
	@Override
	public RiakFuture put(final RiakObject<byte[]> content,
			final PutOptions options, final SiblingHandler handler) {
		notNull(content, "content");
		notNull(options, "options");
		notNull(handler, "handler");

		HttpRequest request = this.factory.newPutRequest(content, options);

		final String procedure = "put/sibling";
		return handle(procedure, request, handler,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) throws Exception {
						if (receive instanceof HttpResponse) {
							HttpResponse response = (HttpResponse) receive;
							if (NettyUtil.isSuccessful(response.getStatus())) {
								try {
									handler.begin();
									RiakObject<byte[]> ro = RestRiakOperations.this.factory
											.convert(response,
													response.getContent(),
													content.getLocation());
									handler.handle(RestRiakOperations.this.support
											.newResponse(ro));
								} finally {
									handler.end(RestRiakOperations.this.support
											.newResponse());
								}
							} else if (response.getStatus().getCode() == 300) {
								dispatchToGetSibling(content.getLocation(),
										options, handler);
							}
							return true;
						}
						return false;
					}
				});
	}

	protected void dispatchToGetSibling(Location location,
			final PutOptions options, SiblingHandler handler) {
		get(location, new GetOptions() {
			@Override
			public Quorum getReadQuorum() {
				return options.getReadQuorum();
			}

			@Override
			public String getIfNoneMatch() {
				return options.getIfNoneMatch();
			}

			@Override
			public String getIfMatch() {
				return options.getIfMatch();
			}

			@Override
			public Date getIfModifiedSince() {
				return options.getIfModifiedSince();
			}
		}, handler);
	}

	@Override
	public RiakFuture post(RiakObject<byte[]> content,
			final RiakResponseHandler<RiakObject<byte[]>> handler) {
		notNull(content, "content");
		notNull(handler, "handler");

		HttpRequest request = this.factory.newPostRequest(content);
		final String procedure = "post";
		return _post(procedure, content, handler, request);
	}

	private RiakFuture _post(final String procedure,
			RiakObject<byte[]> content,
			final RiakResponseHandler<RiakObject<byte[]>> handler,
			HttpRequest request) {
		final RiakObject<byte[]> copied = new DefaultRiakObject(content);

		return handle(procedure, request, handler,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) throws Exception {
						if (receive instanceof HttpResponse) {
							HttpResponse response = (HttpResponse) receive;
							if (NettyUtil.isSuccessful(response.getStatus())) {
								Location location = to(response);
								if (location != null) {
									copied.setLocation(location);
									handler.handle(RestRiakOperations.this.support
											.newResponse(copied));
									return true;
								}
							}
						}
						return false;
					}
				});
	}

	protected Location to(HttpMessage response) {
		String loc = response.getHeader(HttpHeaders.Names.LOCATION);
		if (loc != null && loc.isEmpty() == false) {
			String[] slashed = loc.split("/");
			if (slashed != null && 3 < slashed.length) {
				Location location = new Location(slashed[2], slashed[3]);
				return location;
			}
		}
		return null;
	}

	@Override
	public RiakFuture post(RiakObject<byte[]> content, PutOptions options,
			final RiakResponseHandler<RiakObject<byte[]>> handler) {
		notNull(content, "content");
		notNull(options, "options");
		notNull(handler, "handler");

		HttpRequest request = this.factory.newPostRequest(content, options);
		if (options.getReturnBody() == false) {
			return _post("post/opt", content, handler, request);
		}
		final String procedure = "post/returnbody";
		return handle(procedure, request, handler,
				new NettyUtil.ChunkedMessageAggregator(procedure,
						new NettyUtil.ChunkedMessageHandler() {
							@Override
							public void handle(HttpResponse response,
									ChannelBuffer buffer) throws Exception {
								Location location = to(response);
								if (location == null) {
									// TODO ...
									throw new IllegalStateException();
								}
								RiakObject<byte[]> ro = RestRiakOperations.this.factory
										.convert(response, buffer, location);
								handler.handle(RestRiakOperations.this.support
										.newResponse(ro));

							}
						}));
	}

	@Override
	public RiakFuture delete(Location location,
			final RiakResponseHandler<_> handler) {
		notNull(location, "location");
		notNull(handler, "handler");

		HttpRequest request = this.factory.newDeleteRequest(location);
		return _delete("delete", handler, request);
	}

	protected RiakFuture _delete(String name,
			final RiakResponseHandler<_> handler, HttpRequest request) {
		final String procedure = name;
		return handle(procedure, request, handler);
	}

	@Override
	public RiakFuture delete(Location location, Quorum readWrite,
			RiakResponseHandler<_> handler) {
		notNull(location, "location");
		notNull(readWrite, "readWrite");
		notNull(handler, "handler");

		HttpRequest request = this.factory
				.newDeleteRequest(location, readWrite);
		return _delete("delete/quorum", handler, request);
	}

	@Override
	public RiakFuture mapReduce(MapReduceQueryConstructor constructor,
			RiakResponseHandler<MapReduceResponse> handler) {
		notNull(constructor, "constructor");
		notNull(handler, "handler");

		DefaultMapReduceQuery query = new DefaultMapReduceQuery();
		constructor.cunstruct(query);

		HttpRequest request = this.factory.newMapReduceRequest();
		ChannelBuffer buffer = ChannelBuffers.dynamicBuffer(1024);
		query.prepare(new ChannelBufferOutputStream(buffer));
		HttpHeaders.setContentLength(request, buffer.readableBytes());
		request.setContent(buffer);

		return mapReduce(request, handler);
	}

	protected RiakFuture mapReduce(HttpRequest request,
			final RiakResponseHandler<MapReduceResponse> handler) {
		final String procedure = "mapReduce";
		return handle(procedure, request, handler,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) throws Exception {
						if (receive instanceof HttpResponse) {
							return false;
						} else if (receive instanceof HttpChunk) {
							HttpChunk chunk = (HttpChunk) receive;
							boolean done = chunk.isLast();
							ObjectNode node = to(chunk.getContent());
							MapReduceResponse response = new RestMapReduceResponse(
									node, done);
							handler.handle(RestRiakOperations.this.support
									.newResponse(response));
							return done;
						}
						return false;
					}
				});
	}

	@Override
	public RiakFuture mapReduce(String rawJson,
			RiakResponseHandler<MapReduceResponse> handler) {
		notNull(rawJson, "rawJson");
		notNull(handler, "handler");
		HttpRequest request = this.factory.newMapReduceRequest();
		HttpHeaders.setContentLength(request, rawJson.length());
		request.setContent(ChannelBuffers.wrappedBuffer(rawJson.getBytes()));

		return mapReduce(request, handler);
	}

	@Override
	public RiakFuture walk(Location walkbegin, List<LinkCondition> conditions,
			final RiakResponseHandler<List<RiakObject<byte[]>>> handler) {
		notNull(walkbegin, "walkbegin");
		notNull(conditions, "conditions");
		notNull(handler, "handler");

		HttpRequest request = this.factory.newWalkRequst(walkbegin, conditions);
		final String procedure = "walk";
		return handle(procedure, request, handler,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) throws Exception {
						if (receive instanceof HttpResponse) {
							// do nothing
							return false;
						} else if (receive instanceof PartMessage) {
							PartMessage part = (PartMessage) receive;
							boolean done = part.isLast();
							if (done == false) {
								notifyStep(part, handler);
							}
							return done;
						}
						return false;
					}
				});
	}

	protected void notifyStep(PartMessage message,
			RiakResponseHandler<List<RiakObject<byte[]>>> handler)
			throws Exception {
		MultipartResponseDecoder decoder = new MultipartResponseDecoder();
		if (decoder.setUpBoundary(message)) {
			List<RiakObject<byte[]>> list = new ArrayList<RiakObject<byte[]>>();
			ChannelBuffer buffer = message.getContent();
			while (buffer.readable()) {
				PartMessage msg = decoder.parse(buffer);
				if (msg.isLast()) {
					break;
				} else {
					Location location = to(msg);
					if (location != null) {
						RiakObject<byte[]> ro = this.factory.convert(msg,
								msg.getContent(), location);
						list.add(ro);
					}
				}
			}
			handler.handle(this.support.newResponse(list));
		}
	}

	@Override
	public RiakFuture getStats(final RiakResponseHandler<ObjectNode> handler) {
		notNull(handler, "handler");

		HttpRequest request = this.factory.newGetStatsRequest();
		final String procedure = "getStats";
		return handle(procedure, request, handler,
				new NettyUtil.MessageHandler() {

					@Override
					public boolean handle(Object receive) throws Exception {
						if (receive instanceof HttpResponse) {
							HttpResponse response = (HttpResponse) receive;
							if (NettyUtil.isSuccessful(response.getStatus())) {
								ObjectNode node = to(response.getContent());
								handler.handle(RestRiakOperations.this.support
										.newResponse(node));
								return true;
							}
						}
						return false;
					}
				});
	}

	protected RiakFuture handle(final String name, Object send,
			final RiakResponseHandler<_> users) {
		return this.support.handle(name, send, users, new SimpleMessageHandler(
				name, users, this.support));
	}

	protected <T> RiakFuture handle(final String name, Object send,
			final RiakResponseHandler<T> users,
			final NettyUtil.MessageHandler internal) {
		return this.support.handle(name, send, users,
				new ContinuousMessageHandler<T>(users, internal, this.support));
	}

	@Override
	public RiakFuture getStream(String key, final StreamResponseHandler handler) {
		notNull(key, "key");
		notNull(handler, "handler");

		HttpRequest request = this.factory.newGetStreamRequest(key);
		final String procedure = "getStream";
		return _getStream(procedure, request, handler);
	}

	protected RiakFuture _getStream(final String procedure,
			HttpRequest request, final StreamResponseHandler handler) {
		return handle(procedure, request, handler,
				new NettyUtil.MessageHandler() {

					@Override
					public boolean handle(Object receive) throws Exception {
						if (receive instanceof HttpResponse) {
							HttpResponse response = (HttpResponse) receive;
							boolean done = response.isChunked() == false;
							RiakObject<_> ro = new AbstractRiakObject<_>() {
								@Override
								public _ getContent() {
									return _._;
								}
							};
							RestRiakOperations.this.factory.convertHeaders(
									response, ro);
							handler.begin(ro);
							if (done) {
								try {
									handler.handle(RestRiakOperations.this.support
											.newResponse(response.getContent()));
								} finally {
									handler.end();
								}
							}
							return done;
						} else if (receive instanceof HttpChunk) {
							HttpChunk chunk = (HttpChunk) receive;
							boolean done = chunk.isLast();
							if (done) {
								handler.end();
							} else {
								handler.handle(RestRiakOperations.this.support
										.newResponse(chunk.getContent()));
							}
							return done;
						}

						return false;
					}
				});
	}

	@Override
	public RiakFuture getStream(String key, Range range,
			StreamResponseHandler handler) {
		notNull(key, "key");
		notNull(range, "range");
		notNull(handler, "handler");

		HttpRequest request = this.factory.newGetStreamRequest(key);
		request.setHeader(HttpHeaders.Names.RANGE, range.toRangeSpec());
		LOG.debug(Markers.BOUNDARY, request.toString());
		final String procedure = "getStream/range";
		return _getStream(procedure, request, handler);
	}

	@Override
	public RiakFuture postStream(final RiakObject<InputStreamHandler> content,
			final RiakResponseHandler<String> handler) {
		notNull(content, "content");
		notNull(handler, "handler");

		HttpRequest request = this.factory.newStreamRequest(content, "",
				HttpMethod.POST);

		final String procedure = "postStream";
		return handle(procedure, request, handler,
				new NettyUtil.MessageHandler() {

					@Override
					public boolean handle(Object receive) throws Exception {
						if (receive instanceof HttpResponse) {
							HttpResponse response = (HttpResponse) receive;
							HttpResponseStatus status = response.getStatus();
							if (HttpResponseStatus.CONTINUE.equals(status)) {
								InputStreamHandler ish = content.getContent();
								RestRiakOperations.this.channel
										.write(new ChunkedStream(ish.open()));
								return false;
							} else if (HttpResponseStatus.CREATED
									.equals(status)) {
								String loc = response
										.getHeader(HttpHeaders.Names.LOCATION);
								if (StringUtil.isEmpty(loc) == false
										&& loc.startsWith("/"
												+ RestRiakOperations.this.config
														.getLuwakName() + "/")) {
									final String newKey = loc.substring(7);
									handler.handle(RestRiakOperations.this.support
											.newResponse(newKey));
									return true;
								}
							}
						}
						return false;
					}
				});
	}

	@Override
	public RiakFuture putStream(final RiakObject<InputStreamHandler> content,
			final RiakResponseHandler<_> handler) {
		notNull(content, "content");
		notNull(handler, "handler");

		HttpRequest request = this.factory.newStreamRequest(content, content
				.getLocation().getKey(), HttpMethod.PUT);
		final String procedure = "putStream";
		return handle(procedure, request, handler,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) throws Exception {
						if (receive instanceof HttpResponse) {
							HttpResponse response = (HttpResponse) receive;
							HttpResponseStatus status = response.getStatus();
							if (HttpResponseStatus.CONTINUE.equals(status)) {
								InputStreamHandler ish = content.getContent();
								RestRiakOperations.this.channel
										.write(new ChunkedStream(ish.open()));
								return false;
							} else if (NettyUtil.isSuccessful(status)) {
								handler.handle(RestRiakOperations.this.support
										.newResponse());
								return true;
							}
						}
						return false;
					}
				});
	}

	@Override
	public RiakFuture delete(String key, RiakResponseHandler<_> handler) {
		notNull(key, "key");
		notNull(handler, "handler");

		HttpRequest request = this.factory.newDeleteRequest(key);
		return handle("delete/luwak", request, handler);
	}

}