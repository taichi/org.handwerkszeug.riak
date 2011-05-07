package org.handwerkszeug.riak.http.rest;

import static org.handwerkszeug.riak.util.Validation.notNull;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.handwerkszeug.riak.mapreduce.DefaultMapReduceQuery;
import org.handwerkszeug.riak.mapreduce.MapReduceQueryConstructor;
import org.handwerkszeug.riak.mapreduce.MapReduceResponse;
import org.handwerkszeug.riak.model.AbstractRiakObject;
import org.handwerkszeug.riak.model.Bucket;
import org.handwerkszeug.riak.model.DefaultRiakObject;
import org.handwerkszeug.riak.model.GetOptions;
import org.handwerkszeug.riak.model.KeyResponse;
import org.handwerkszeug.riak.model.Link;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.PutOptions;
import org.handwerkszeug.riak.model.Quorum;
import org.handwerkszeug.riak.model.Range;
import org.handwerkszeug.riak.model.RiakFuture;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.model.RiakResponse;
import org.handwerkszeug.riak.nls.Messages;
import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.handwerkszeug.riak.op.SiblingHandler;
import org.handwerkszeug.riak.op.internal.CompletionSupport;
import org.handwerkszeug.riak.op.internal.IncomprehensibleProtocolException;
import org.handwerkszeug.riak.util.HttpUtil;
import org.handwerkszeug.riak.util.JsonUtil;
import org.handwerkszeug.riak.util.NettyUtil;
import org.handwerkszeug.riak.util.StringUtil;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMessage;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.codec.http.MultipartResponseDecoder;
import org.jboss.netty.handler.codec.http.PartMessage;
import org.jboss.netty.handler.codec.http.QueryStringEncoder;
import org.jboss.netty.handler.stream.ChunkedStream;
import org.jboss.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author taichi
 */
public class RestRiakOperations implements HttpRiakOperations {

	static final Logger LOG = LoggerFactory.getLogger(RestRiakOperations.class);

	String host;
	String riakPath;
	// for postStream.
	Channel channel;
	CompletionSupport support;

	String clientId;

	ObjectMapper objectMapper = new ObjectMapper();

	public RestRiakOperations(String host, String riakPath, Channel channel) {
		notNull(host, "host");
		notNull(channel, "channel");
		this.host = removeSlashIfNeed(host);
		this.channel = channel;
		this.support = new CompletionSupport(channel);
		this.riakPath = riakPath;
	}

	protected String removeSlashIfNeed(String uri) {
		return uri.endsWith("/") ? uri.substring(0, uri.length() - 1) : uri;
	}

	@Override
	public RiakFuture ping(final RiakResponseHandler<String> handler) {
		notNull(handler, "handler");

		HttpRequest request = build("/ping", HttpMethod.GET);
		final String procedure = "ping";
		return handle(procedure, request, handler,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) throws Exception {
						if (receive instanceof HttpResponse) {
							HttpResponse response = (HttpResponse) receive;
							if (NettyUtil.isSuccessful(response.getStatus())) {
								handler.handle(support.newResponse("pong"));
								return true;
							}
						}
						throw new IncomprehensibleProtocolException(procedure);
					}
				});
	}

	@Override
	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	@Override
	public String getClientId() {
		return this.clientId;
	}

	protected HttpRequest build(String path, HttpMethod method) {
		return build(this.host + "/" + this.riakPath, path, method);
	}

	protected HttpRequest build(String app, String path, HttpMethod method) {
		try {
			URI uri = new URI(app + path);

			LOG.debug(Markers.BOUNDARY, uri.toASCIIString());
			HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
					method, uri.toASCIIString());
			request.setHeader(HttpHeaders.Names.HOST, uri.getHost());
			request.setHeader(HttpHeaders.Names.CONNECTION,
					HttpHeaders.Values.KEEP_ALIVE);
			if (StringUtil.isEmpty(this.clientId) == false) {
				request.setHeader(RiakHttpHeaders.CLIENT_ID, this.clientId);
			}
			return request;
		} catch (URISyntaxException e) {
			throw new RiakException(e);
		}
	}

	@Override
	public RiakFuture listBuckets(
			final RiakResponseHandler<List<String>> handler) {
		notNull(handler, "handler");

		HttpRequest request = build("?buckets=true", HttpMethod.GET);
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
								handler.handle(support.newResponse(list));
								return true;
							}
						}
						throw new IncomprehensibleProtocolException(procedure);
					}
				});
	}

	@SuppressWarnings("unchecked")
	<T extends JsonNode> T to(ChannelBuffer buffer, T... t) {
		try {
			if (buffer != null && buffer.readable()) {
				JsonNode node = this.objectMapper
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

		HttpRequest request = build("/" + bucket + "?props=false&keys=stream",
				HttpMethod.GET);
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
						throw new IncomprehensibleProtocolException(procedure);
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
				handler.handle(support.newResponse(kr));
			}
		}
	}

	@Override
	public RiakFuture getBucket(String bucket,
			final RiakResponseHandler<Bucket> handler) {
		notNull(bucket, "bucket");
		notNull(handler, "handler");

		HttpRequest request = build("/" + bucket + "?props=true",
				HttpMethod.GET);

		final String procedure = "getBucket";
		return handle(procedure, request, handler,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) throws Exception {
						if (receive instanceof HttpResponse) {
							HttpResponse response = (HttpResponse) receive;
							if (NettyUtil.isSuccessful(response.getStatus())) {
								BucketHolder holder = objectMapper.readValue(
										new ChannelBufferInputStream(response
												.getContent()),
										BucketHolder.class);
								handler.handle(support
										.newResponse(holder.props));
								return true;
							}
						}
						throw new IncomprehensibleProtocolException(procedure);
					}
				});
	}

	@Override
	public RiakFuture setBucket(Bucket bucket,
			final RiakResponseHandler<_> handler) {
		notNull(bucket, "bucket");
		notNull(handler, "handler");

		HttpRequest request = buildSetBucketRequest(bucket);
		final String procedure = "setBucket";
		return handle(procedure, request, handler);
	}

	protected HttpRequest buildSetBucketRequest(Bucket bucket) {
		try {
			BucketHolder holder = new BucketHolder();
			holder.props = bucket;
			ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
			OutputStream out = new ChannelBufferOutputStream(buffer);
			objectMapper.writeValue(out, holder);
			HttpRequest request = build("/" + bucket.getName(), HttpMethod.PUT);
			request.setHeader(HttpHeaders.Names.CONTENT_LENGTH,
					buffer.readableBytes());
			request.setHeader(HttpHeaders.Names.CONTENT_TYPE,
					RiakHttpHeaders.CONTENT_JSON);
			request.setHeader(HttpHeaders.Names.ACCEPT,
					RiakHttpHeaders.CONTENT_JSON);
			request.setContent(buffer);
			return request;
		} catch (IOException e) {
			throw new RiakException(e);
		}
	}

	@Override
	public RiakFuture get(Location location,
			RiakResponseHandler<RiakObject<byte[]>> handler) {
		notNull(location, "location");
		notNull(handler, "handler");

		return getSingle(buildGetRequst(location), location, handler);
	}

	@Override
	public RiakFuture get(Location location, GetOptions options,
			RiakResponseHandler<RiakObject<byte[]>> handler) {
		notNull(location, "location");
		notNull(options, "options");
		notNull(handler, "handler");

		return getSingle(buildGetRequst(location, options), location, handler);
	}

	protected HttpRequest buildGetRequst(Location location) {
		HttpRequest request = build(
				"/" + location.getBucket() + "/" + location.getKey(),
				HttpMethod.GET);
		return request;
	}

	protected HttpRequest buildGetRequst(Location location, GetOptions options) {
		HttpRequest request = buildGetRequst(location);
		QueryStringEncoder params = new QueryStringEncoder(request.getUri());
		if (options.getReadQuorum() != null) {
			params.addParam("r", options.getReadQuorum().getString());
		}
		// TODO PR support.

		if (StringUtil.isEmpty(options.getIfNoneMatch()) == false) {
			request.setHeader(HttpHeaders.Names.IF_NONE_MATCH,
					options.getIfNoneMatch());
		}

		if (StringUtil.isEmpty(options.getIfMatch()) == false) {
			request.setHeader(HttpHeaders.Names.IF_MATCH, options.getIfMatch());
		}

		if (options.getIfModifiedSince() != null) {
			request.setHeader(HttpHeaders.Names.IF_MODIFIED_SINCE,
					HttpUtil.format(options.getIfModifiedSince()));
		}

		request.setUri(params.toString());
		return request;
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
								RiakObject<byte[]> ro = convert(response,
										buffer, location);
								handler.handle(support.newResponse(ro));
							}
						}));
	}

	protected RiakObject<byte[]> convert(HttpMessage headers,
			ChannelBuffer buffer, Location location) {
		DefaultRiakObject ro = new DefaultRiakObject(location);
		ro.setContent(buffer.array());

		convertHeaders(headers, ro);

		return ro;
	}

	protected <T> void convertHeaders(HttpMessage headers, RiakObject<T> ro) {
		ro.setVectorClock(headers.getHeader(RiakHttpHeaders.VECTOR_CLOCK));

		ro.setContentType(headers.getHeader(HttpHeaders.Names.CONTENT_TYPE));

		// NOP ro.setCharset(charset);

		ro.setContentEncoding(headers
				.getHeader(HttpHeaders.Names.CONTENT_ENCODING));

		// NOP ro.setVtag(vtag);

		List<String> links = headers.getHeaders(RiakHttpHeaders.LINK);
		ro.setLinks(parse(links));

		String lastmod = headers.getHeader(HttpHeaders.Names.LAST_MODIFIED);
		if (StringUtil.isEmpty(lastmod) == false) {
			Date d = HttpUtil.parse(lastmod);
			ro.setLastModified(d);
			if (LOG.isDebugEnabled()) {
				LOG.debug(Markers.DETAIL, Messages.LastModified, lastmod);
			}
		}

		Map<String, String> map = new HashMap<String, String>();
		for (String name : headers.getHeaderNames()) {
			if (RiakHttpHeaders.isUsermeta(name)) {
				String key = RiakHttpHeaders.fromUsermeta(name);
				map.put(key, headers.getHeader(name));
			}
		}
		ro.setUserMetadata(map);
	}

	static final Pattern LINK_PATTERN = Pattern
			.compile("</\\w+/(\\w+)/(\\w+)>;\\s+riaktag=\"([^\"\\r\\n]+)\"");
	static final int LINK_BUCKET = 1;
	static final int LINK_KEY = 2;
	static final int LINK_TAG = 3;

	protected List<Link> parse(List<String> links) {
		List<Link> result = new ArrayList<Link>();
		for (String raw : links) {
			Matcher m = LINK_PATTERN.matcher(raw);
			while (m.find()) {
				String b = m.group(LINK_BUCKET);
				String k = m.group(LINK_KEY);
				String t = m.group(LINK_TAG);
				if (b != null && k != null && t != null) {
					Link l = new Link(new Location(b, k), t);
					result.add(l);
				}
			}
		}
		return result;
	}

	@Override
	public RiakFuture get(final Location location, GetOptions options,
			final SiblingHandler handler) {
		notNull(location, "location");
		notNull(options, "options");
		notNull(handler, "handler");

		HttpRequest request = buildGetRequst(location, options);
		request.setHeader(HttpHeaders.Names.ACCEPT, RiakHttpHeaders.MULTI_PART);

		final String procedure = "get/sibling";
		return handle(procedure, request, handler,
				new NettyUtil.MessageHandler() {
					String vclock;

					@Override
					public boolean handle(Object receive) throws Exception {
						if (receive instanceof HttpResponse) {
							HttpResponse response = (HttpResponse) receive;
							vclock = response
									.getHeader(RiakHttpHeaders.VECTOR_CLOCK);
							handler.begin();
							return false;
						} else if (receive instanceof PartMessage) {
							PartMessage part = (PartMessage) receive;
							boolean done = part.isLast();
							part.setHeader(RiakHttpHeaders.VECTOR_CLOCK, vclock);

							if (done) {
								handler.end();
							} else {
								RiakObject<byte[]> ro = convert(part,
										part.getContent(), location);
								handler.handle(support.newResponse(ro));
							}
							return done;
						}
						throw new IncomprehensibleProtocolException(procedure);
					}
				});
	}

	@Override
	public RiakFuture put(RiakObject<byte[]> content,
			final RiakResponseHandler<_> handler) {
		notNull(content, "content");
		notNull(handler, "handler");

		HttpRequest request = buildPutRequest(content);

		final String procedure = "put";
		return handle(procedure, request, handler);
	}

	protected HttpRequest buildPutRequest(RiakObject<byte[]> content) {
		Location location = content.getLocation();
		HttpRequest request = build(
				"/" + location.getBucket() + "/" + location.getKey(),
				HttpMethod.PUT);
		merge(request, content);

		return request;
	}

	protected void merge(HttpRequest request, RiakObject<byte[]> content) {
		if (StringUtil.isEmpty(content.getVectorClock()) == false) {
			request.setHeader(RiakHttpHeaders.VECTOR_CLOCK,
					content.getVectorClock());
		}

		ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(content
				.getContent());
		request.setHeader(HttpHeaders.Names.CONTENT_LENGTH,
				buffer.readableBytes());
		request.setContent(buffer);

		mergeHeaders(request, content);
	}

	protected <T> void mergeHeaders(HttpRequest request, RiakObject<T> content) {
		if (StringUtil.isEmpty(content.getContentType()) == false) {
			request.setHeader(HttpHeaders.Names.CONTENT_TYPE,
					content.getContentType());
		}

		// NOP content.getCharset();

		if (StringUtil.isEmpty(content.getContentEncoding()) == false) {
			request.setHeader(HttpHeaders.Names.CONTENT_ENCODING,
					content.getContentEncoding());
		}

		// NOP content.getVtag();

		if ((content.getLinks() != null)
				&& (content.getLinks().isEmpty() == false)) {
			addLinkHeader(request, content);
		}

		if (content.getLastModified() != null) {
			request.setHeader(HttpHeaders.Names.LAST_MODIFIED,
					HttpUtil.format(content.getLastModified()));
		}

		if ((content.getUserMetadata() != null)
				&& (content.getUserMetadata().isEmpty() == false)) {
			Map<String, String> map = content.getUserMetadata();
			for (String key : map.keySet()) {
				request.setHeader(RiakHttpHeaders.toUsermeta(key), map.get(key));
			}
		}
	}

	protected <T> void addLinkHeader(HttpRequest request, RiakObject<T> content) {
		StringBuilder stb = new StringBuilder();
		for (Link link : content.getLinks()) {
			if (0 < stb.length()) {
				stb.append(", ");
			}
			stb.append('<');
			stb.append('/');
			stb.append(this.riakPath);
			stb.append('/');
			stb.append(link.getLocation().getBucket());
			stb.append('/');
			stb.append(link.getLocation().getKey());
			stb.append(">; riaktag=\"");
			stb.append(link.getTag());
			stb.append('"');

			// https://github.com/basho/riak-java-client/pull/7
			// MochiWeb has problem of too long header ?
			if (2000 < stb.length()) {
				request.addHeader(RiakHttpHeaders.LINK, stb.toString());
				stb = new StringBuilder();
			}
		}
		if (0 < stb.length()) {
			request.addHeader(RiakHttpHeaders.LINK, stb.toString());
		}
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

		HttpRequest request = buildPutRequest(content, options);

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
									RiakObject<byte[]> ro = convert(response,
											response.getContent(),
											content.getLocation());
									handler.handle(support.newResponse(ro));
								} finally {
									handler.end();
								}
							} else if (response.getStatus().getCode() == 300) {
								dispatchToGetSibling(content.getLocation(),
										options, handler);
							}
							return true;
						}
						throw new IncomprehensibleProtocolException(procedure);
					}
				});
	}

	protected HttpRequest buildPutRequest(RiakObject<byte[]> content,
			PutOptions options) {
		HttpRequest request = buildPutRequest(content);

		QueryStringEncoder params = to(options, request);
		request.setUri(params.toString());
		return request;
	}

	protected QueryStringEncoder to(PutOptions options, HttpRequest request) {
		QueryStringEncoder params = new QueryStringEncoder(request.getUri());

		if (options.getReadQuorum() != null) {
			// PBC-API does't support this parameter. why not?
			params.addParam("r", options.getReadQuorum().getString());
		}
		if (options.getWriteQuorum() != null) {
			params.addParam("w", options.getWriteQuorum().getString());
		}
		if (options.getDurableWriteQuorum() != null) {
			params.addParam("dw", options.getDurableWriteQuorum().getString());
		}
		if (options.getReturnBody()) {
			params.addParam("returnbody",
					String.valueOf(options.getReturnBody()));
		}
		return params;
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

		HttpRequest request = buildPostRequest(content);
		return _post(content, handler, request);
	}

	private RiakFuture _post(RiakObject<byte[]> content,
			final RiakResponseHandler<RiakObject<byte[]>> handler,
			HttpRequest request) {
		final RiakObject<byte[]> copied = copy(content);
		final String procedure = "post";
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
									handler.handle(support.newResponse(copied));
									return true;
								}
							}
						}
						throw new IncomprehensibleProtocolException(procedure);
					}
				});
	}

	protected RiakObject<byte[]> copy(RiakObject<byte[]> src) {
		DefaultRiakObject ro = new DefaultRiakObject(src.getLocation());
		ro.setContent(Arrays.copyOf(src.getContent(), src.getContent().length));
		ro.setVectorClock(src.getVectorClock());
		ro.setContentType(src.getContentType());
		ro.setCharset(src.getCharset());
		ro.setContentEncoding(src.getContentEncoding());
		ro.setVtag(src.getVtag());
		List<Link> links = src.getLinks();
		if (links != null && links.isEmpty() == false) {
			ro.setLinks(new ArrayList<Link>(links));
		}
		Date d = src.getLastModified();
		if (d != null) {
			ro.setLastModified(new Date(d.getTime()));
		}
		Map<String, String> metas = src.getUserMetadata();
		if (metas != null && metas.isEmpty() == false) {
			ro.setUserMetadata(new HashMap<String, String>(metas));
		}
		return ro;
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

	protected HttpRequest buildPostRequest(RiakObject<byte[]> content) {
		HttpRequest request = build("/" + content.getLocation().getBucket(),
				HttpMethod.POST);
		merge(request, content);
		return request;
	}

	@Override
	public RiakFuture post(RiakObject<byte[]> content, PutOptions options,
			final RiakResponseHandler<RiakObject<byte[]>> handler) {
		notNull(content, "content");
		notNull(options, "options");
		notNull(handler, "handler");

		HttpRequest request = buildPostRequest(content, options);
		if (options.getReturnBody() == false) {
			return _post(content, handler, request);
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
									throw new IncomprehensibleProtocolException(
											procedure);
								}
								RiakObject<byte[]> ro = convert(response,
										buffer, location);
								handler.handle(support.newResponse(ro));

							}
						}));
	}

	protected HttpRequest buildPostRequest(RiakObject<byte[]> content,
			PutOptions options) {
		HttpRequest request = buildPostRequest(content);
		QueryStringEncoder params = to(options, request);
		request.setUri(params.toString());
		return request;
	}

	@Override
	public RiakFuture delete(Location location,
			final RiakResponseHandler<_> handler) {
		notNull(location, "location");
		notNull(handler, "handler");

		HttpRequest request = buildDeleteRequest(location);
		return _delete("delete", handler, request);
	}

	protected HttpRequest buildDeleteRequest(Location location) {
		HttpRequest request = build(
				"/" + location.getBucket() + "/" + location.getKey(),
				HttpMethod.DELETE);
		return request;
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

		HttpRequest request = buildDeleteRequest(location, readWrite);
		return _delete("delete/quorum", handler, request);
	}

	protected HttpRequest buildDeleteRequest(Location location, Quorum readWrite) {
		HttpRequest request = buildDeleteRequest(location);
		QueryStringEncoder params = new QueryStringEncoder(request.getUri());
		params.addParam("rw", readWrite.getString());
		request.setUri(params.toString());
		return request;
	}

	@Override
	public RiakFuture mapReduce(MapReduceQueryConstructor constructor,
			RiakResponseHandler<MapReduceResponse> handler) {
		notNull(constructor, "constructor");
		notNull(handler, "handler");

		DefaultMapReduceQuery query = new DefaultMapReduceQuery();
		constructor.cunstruct(query);

		HttpRequest request = buildMapReduceRequest();
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
							handler.handle(support.newResponse(response));
							return done;
						}
						throw new IncomprehensibleProtocolException(procedure);
					}
				});
	}

	protected HttpRequest buildMapReduceRequest() {
		HttpRequest request = build(this.host, "/mapred?chunked=true",
				HttpMethod.POST);
		request.setHeader(HttpHeaders.Names.CONTENT_TYPE,
				RiakHttpHeaders.CONTENT_JSON);

		return request;
	}

	@Override
	public RiakFuture mapReduce(String rawJson,
			RiakResponseHandler<MapReduceResponse> handler) {
		notNull(rawJson, "rawJson");
		notNull(handler, "handler");
		HttpRequest request = buildMapReduceRequest();
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

		HttpRequest request = buildWalkRequst(walkbegin, conditions);
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
						throw new IncomprehensibleProtocolException(procedure);
					}
				});
	}

	protected HttpRequest buildWalkRequst(Location walkbegin,
			List<LinkCondition> conditions) {
		StringBuilder stb = new StringBuilder(64);
		stb.append('/');
		stb.append(walkbegin.getBucket());
		stb.append('/');
		stb.append(walkbegin.getKey());
		for (LinkCondition cond : conditions) {
			stb.append('/');
			stb.append(cond.getBucket());
			stb.append(',');
			stb.append(cond.getTag());
			stb.append(',');
			if (cond.getKeep()) {
				stb.append('1');
			} else {
				stb.append('_');
			}
		}
		return build(stb.toString(), HttpMethod.GET);
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
						RiakObject<byte[]> ro = convert(msg, msg.getContent(),
								location);
						list.add(ro);
					}
				}
			}
			handler.handle(support.newResponse(list));
		}
	}

	@Override
	public RiakFuture getStats(final RiakResponseHandler<ObjectNode> handler) {
		notNull(handler, "handler");

		HttpRequest request = buildGetStatsRequest();
		final String procedure = "getStats";
		return handle(procedure, request, handler,
				new NettyUtil.MessageHandler() {

					@Override
					public boolean handle(Object receive) throws Exception {
						if (receive instanceof HttpResponse) {
							HttpResponse response = (HttpResponse) receive;
							if (NettyUtil.isSuccessful(response.getStatus())) {
								ObjectNode node = to(response.getContent());
								handler.handle(support.newResponse(node));
								return true;
							}
						}
						throw new IncomprehensibleProtocolException(procedure);
					}
				});
	}

	protected HttpRequest buildGetStatsRequest() {
		HttpRequest request = build(this.host, "/stats", HttpMethod.GET);
		return request;
	}

	protected RiakFuture handle(final String name, Object send,
			final RiakResponseHandler<_> users) {
		return this.support.handle(name, send, users,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) throws Exception {
						if (receive instanceof HttpResponse) {
							HttpResponse response = (HttpResponse) receive;
							HttpResponseStatus status = response.getStatus();
							if (NettyUtil.isError(status)) {
								users.onError(new RestErrorResponse(response));
								return true;
							}
							if (NettyUtil.isSuccessful(status)) {
								users.handle(support.newResponse());
								return true;
							}
						}
						throw new IncomprehensibleProtocolException(name);
					}
				});
	}

	protected <T> RiakFuture handle(final String name, Object send,
			final RiakResponseHandler<T> users,
			final NettyUtil.MessageHandler internal) {
		return this.support.handle(name, send, users,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) throws Exception {
						if (receive instanceof HttpResponse) {
							HttpResponse response = (HttpResponse) receive;
							if (NettyUtil.isError(response.getStatus())) {
								users.onError(new RestErrorResponse(response));
								return true;
							}
						}
						return internal.handle(receive);
					}
				});
	}

	class RestErrorResponse implements RiakResponse {

		final HttpResponse master;

		public RestErrorResponse(HttpResponse master) {
			this.master = master;
		}

		@Override
		public int getResponseCode() {
			return this.master.getStatus().getCode();
		}

		@Override
		public String getMessage() {
			ChannelBuffer content = this.master.getContent();
			if (content.readable()) {
				return content.toString(CharsetUtil.UTF_8);
			}
			return "";
		}

		@Override
		public void operationComplete() {
			support.complete();
		}
	}

	@Override
	public RiakFuture getStream(String key, final StreamResponseHandler handler) {
		notNull(key, "key");
		notNull(handler, "handler");

		HttpRequest request = buildGetStreamRequest(key);
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
							convertHeaders(response, ro);
							handler.begin(ro);
							if (done) {
								try {
									handler.handle(support.newResponse(response
											.getContent()));
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
								handler.handle(support.newResponse(chunk
										.getContent()));
							}
							return done;
						}

						throw new IncomprehensibleProtocolException(procedure);
					}
				});
	}

	protected HttpRequest buildGetStreamRequest(String key) {
		HttpRequest request = build(this.host, "/luwak/" + key, HttpMethod.GET);
		return request;
	}

	@Override
	public RiakFuture getStream(String key, Range range,
			StreamResponseHandler handler) {
		notNull(key, "key");
		notNull(range, "range");
		notNull(handler, "handler");

		HttpRequest request = buildGetStreamRequest(key);
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

		HttpRequest request = buildStreamRequest(content, "", HttpMethod.POST);

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
								channel.write(new ChunkedStream(ish.open()));
								return false;
							} else if (HttpResponseStatus.CREATED
									.equals(status)) {
								String loc = response
										.getHeader(HttpHeaders.Names.LOCATION);
								if (StringUtil.isEmpty(loc) == false
										&& loc.startsWith("/luwak/")) {
									final String newKey = loc.substring(7);
									handler.handle(support.newResponse(newKey));
									return true;
								}
							}
						}
						throw new IncomprehensibleProtocolException(procedure);
					}
				});
	}

	protected HttpRequest buildStreamRequest(
			final RiakObject<InputStreamHandler> content, String key,
			HttpMethod method) {
		HttpRequest request = build(this.host, "/luwak/" + key, method);
		mergeHeaders(request, content);
		request.setHeader(HttpHeaders.Names.EXPECT, HttpHeaders.Values.CONTINUE);
		request.setHeader(HttpHeaders.Names.CONTENT_LENGTH, content
				.getContent().getContentLength());
		return request;
	}

	@Override
	public RiakFuture putStream(final RiakObject<InputStreamHandler> content,
			final RiakResponseHandler<_> handler) {
		notNull(content, "content");
		notNull(handler, "handler");

		HttpRequest request = buildStreamRequest(content, content.getLocation()
				.getKey(), HttpMethod.PUT);
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
								channel.write(new ChunkedStream(ish.open()));
								return false;
							} else if (NettyUtil.isSuccessful(status)) {
								handler.handle(support.newResponse());
							}
						}
						throw new IncomprehensibleProtocolException(procedure);
					}
				});
	}

	@Override
	public RiakFuture delete(String key, RiakResponseHandler<_> handler) {
		notNull(key, "key");
		notNull(handler, "handler");

		HttpRequest request = build(this.host, "/luwak/" + key,
				HttpMethod.DELETE);
		return handle("delete/luwak", request, handler);
	}

}