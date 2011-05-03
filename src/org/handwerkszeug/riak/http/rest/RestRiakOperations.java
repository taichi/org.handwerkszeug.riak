package org.handwerkszeug.riak.http.rest;

import static org.handwerkszeug.riak.util.Validation.notNull;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.Markers;
import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.http.HttpRiakOperations;
import org.handwerkszeug.riak.http.InputStreamHandler;
import org.handwerkszeug.riak.http.LinkCondition;
import org.handwerkszeug.riak.http.OutputStreamHandler;
import org.handwerkszeug.riak.http.RiakHttpHeaders;
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
import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.model.RiakFuture;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.model.RiakResponse;
import org.handwerkszeug.riak.model.internal.AbstractRiakObjectResponse;
import org.handwerkszeug.riak.model.internal.AbstractRiakResponse;
import org.handwerkszeug.riak.nls.Messages;
import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.handwerkszeug.riak.op.SiblingHandler;
import org.handwerkszeug.riak.op.internal.CompletionSupport;
import org.handwerkszeug.riak.util.HttpUtil;
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
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.codec.http.QueryStringEncoder;
import org.jboss.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author taichi
 */
public class RestRiakOperations implements HttpRiakOperations {

	static final Logger LOG = LoggerFactory.getLogger(RestRiakOperations.class);

	String riakURI;
	String riakPath;
	CompletionSupport support;

	ObjectMapper objectMapper = new ObjectMapper();

	public RestRiakOperations(String riakURI, Channel channel) {
		notNull(riakURI, "riakURI");
		notNull(channel, "channel");
		this.riakURI = removeSlashIfNeed(riakURI);
		this.support = new CompletionSupport(channel);
		this.riakPath = riakURI.substring(this.riakURI.lastIndexOf('/'));
	}

	protected String removeSlashIfNeed(String uri) {
		return uri.endsWith("/") ? uri.substring(0, uri.length() - 1) : uri;
	}

	@Override
	public RiakFuture ping(final RiakResponseHandler<String> handler) {
		notNull(handler, "handler");

		HttpRequest request = build("/ping", HttpMethod.GET);
		return handle("ping", request, handler, new NettyUtil.MessageHandler() {
			@Override
			public boolean handle(Object receive) throws Exception {
				if (receive instanceof HttpResponse) {
					HttpResponse response = (HttpResponse) receive;
					if (NettyUtil.isSuccessful(response.getStatus())) {
						handler.handle(new RestRiakResponse<String>() {
							public String getContents() {
								return "pong";
							};
						});
						return true;
					}
				}
				throw new IllegalStateException();
			}
		});
	}

	protected HttpRequest build(String path, HttpMethod method) {
		try {
			URI uri = new URI(this.riakURI + path);
			LOG.debug(Markers.BOUNDARY, uri.toASCIIString());
			HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
					method, uri.toASCIIString());
			request.setHeader(HttpHeaders.Names.HOST, uri.getHost());
			request.setHeader(HttpHeaders.Names.CONNECTION,
					HttpHeaders.Values.KEEP_ALIVE);

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
		return handle("listBuckets", request, handler,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) throws Exception {
						if (receive instanceof HttpResponse) {
							HttpResponse response = (HttpResponse) receive;
							ChannelBuffer buffer = response.getContent();
							ObjectNode node = to(buffer);
							if (node != null) {
								final List<String> list = to(node
										.get("buckets"));
								handler.handle(new RestRiakResponse<List<String>>() {
									@Override
									public List<String> getContents() {
										return list;
									}
								});
								return true;
							}
						}
						throw new IllegalStateException();
					}
				});
	}

	@SuppressWarnings("unchecked")
	<T extends JsonNode> T to(ChannelBuffer buffer, T... t) {
		try {
			if (buffer.readable()) {
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

	List<String> to(JsonNode node) {
		if (node != null && node.isArray()) {
			ArrayNode an = (ArrayNode) node;
			List<String> list = new ArrayList<String>(an.size());
			for (Iterator<JsonNode> i = an.getElements(); i.hasNext();) {
				String key = i.next().getValueAsText();
				list.add(key);
			}
			return list;
		}
		return Collections.emptyList();
	}

	@Override
	public RiakFuture listKeys(String bucket,
			final RiakResponseHandler<KeyResponse> handler) {
		notNull(bucket, "bucket");
		notNull(handler, "handler");

		HttpRequest request = build("/" + bucket + "?props=false&keys=stream",
				HttpMethod.GET);
		return handle("listKeys", request, handler,
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
						throw new IllegalStateException();
					}
				});
	}

	protected void _listKeys(ChannelBuffer buffer,
			final RiakResponseHandler<KeyResponse> handler) throws Exception {
		ObjectNode on = to(buffer);
		if (on != null) {
			JsonNode node = on.get("keys");
			if (node != null) {
				final List<String> list = to(node);
				final KeyResponse kr = new KeyResponse(list, list.isEmpty());
				handler.handle(new RestRiakResponse<KeyResponse>() {
					@Override
					public KeyResponse getContents() {
						return kr;
					}
				});
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

		return handle("getBucket", request, handler,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) throws Exception {
						if (receive instanceof HttpResponse) {
							HttpResponse response = (HttpResponse) receive;
							if (NettyUtil.isSuccessful(response.getStatus())) {
								final BucketHolder holder = objectMapper
										.readValue(
												new ChannelBufferInputStream(
														response.getContent()),
												BucketHolder.class);
								handler.handle(new RestRiakResponse<Bucket>() {
									@Override
									public Bucket getContents() {
										return holder.props;
									}
								});
								return true;
							}
						}
						throw new IllegalStateException();
					}
				});
	}

	@Override
	public RiakFuture setBucket(Bucket bucket,
			final RiakResponseHandler<_> handler) {
		notNull(bucket, "bucket");
		notNull(handler, "handler");

		HttpRequest request = buildSetBucketRequest(bucket);
		return handle("setBucket", request, handler,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) throws Exception {
						if (receive instanceof HttpResponse) {
							HttpResponse response = (HttpResponse) receive;
							if (NettyUtil.isSuccessful(response.getStatus())) {
								handler.handle(new NoOpResponse());
								return true;
							}
						}
						throw new IllegalStateException();
					}
				});
	}

	protected HttpRequest buildSetBucketRequest(Bucket bucket) {
		try {
			BucketHolder holder = new BucketHolder();
			holder.props = bucket;
			ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
			OutputStream out = new ChannelBufferOutputStream(buffer);
			objectMapper.writeValue(out, holder);
			System.out.println(buffer.toString(CharsetUtil.UTF_8));
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

		return handle("get/single", request, handler,
				new NettyUtil.ChunkedMessageAggregator(
						new NettyUtil.ChunkedMessageHandler() {
							@Override
							public void handle(HttpResponse response,
									ChannelBuffer buffer) throws Exception {
								RiakObject<byte[]> ro = convert(response,
										buffer, location);
								handler.handle(new RestRiakObjectResponse(ro));
							}
						}));
	}

	protected RiakObject<byte[]> convert(HttpResponse response,
			ChannelBuffer buffer, Location location) {
		DefaultRiakObject ro = new DefaultRiakObject(location);
		ro.setContent(buffer.array());

		ro.setVectorClock(response.getHeader(RiakHttpHeaders.VECTOR_CLOCK));

		ro.setContentType(response.getHeader(HttpHeaders.Names.CONTENT_TYPE));

		// NOP ro.setCharset(charset);

		ro.setContentEncoding(response
				.getHeader(HttpHeaders.Names.CONTENT_ENCODING));

		// NOP ro.setVtag(vtag);

		List<String> links = response.getHeaders(RiakHttpHeaders.LINK);
		ro.setLinks(parse(links));

		String lastmod = response.getHeader(HttpHeaders.Names.LAST_MODIFIED);
		if (StringUtil.isEmpty(lastmod) == false) {
			Date d = HttpUtil.parse(lastmod);
			ro.setLastModified(d);
			if (LOG.isDebugEnabled()) {
				LOG.debug(Markers.DETAIL, Messages.LastModified, lastmod);
			}
		}

		Map<String, String> map = new HashMap<String, String>();
		for (String name : response.getHeaderNames()) {
			if (RiakHttpHeaders.isUsermeta(name)) {
				String key = RiakHttpHeaders.fromUsermeta(name);
				map.put(key, response.getHeader(name));
			}
		}
		ro.setUserMetadata(map);

		return ro;
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
	public RiakFuture get(Location location, GetOptions options,
			SiblingHandler handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture put(RiakObject<byte[]> content,
			final RiakResponseHandler<List<RiakObject<byte[]>>> handler) {
		notNull(content, "content");
		notNull(handler, "handler");

		HttpRequest request = buildPutRequest(content);

		return handle("put", request, handler, new NettyUtil.MessageHandler() {
			@Override
			public boolean handle(Object receive) throws Exception {
				if (receive instanceof HttpResponse) {
					HttpResponse response = (HttpResponse) receive;
					HttpResponseStatus status = response.getStatus();
					if (NettyUtil.isSuccessful(status)
							|| status.getCode() == 300) {
						handler.handle(new RestRiakResponse<List<RiakObject<byte[]>>>() {
							@Override
							public List<RiakObject<byte[]>> getContents() {
								return Collections.emptyList();
							}
						});
						return true;
					}
				}
				throw new IllegalStateException();
			}
		});
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

	protected void addLinkHeader(HttpRequest request, RiakObject<byte[]> content) {
		StringBuilder stb = new StringBuilder();
		for (Link link : content.getLinks()) {
			if (0 < stb.length()) {
				stb.append(", ");
			}
			stb.append('<');
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

	@Override
	public RiakFuture put(final RiakObject<byte[]> content, PutOptions options,
			final RiakResponseHandler<List<RiakObject<byte[]>>> handler) {
		notNull(content, "content");
		notNull(options, "options");
		notNull(handler, "handler");

		HttpRequest request = buildPutRequest(content, options);
		return handle("put", request, handler,
				new NettyUtil.ChunkedMessageAggregator(
						new NettyUtil.ChunkedMessageHandler() {
							@Override
							public void handle(HttpResponse response,
									ChannelBuffer buffer) throws Exception {
								if (NettyUtil.isSuccessful(response.getStatus())) {
									RiakObject<byte[]> ro = convert(response,
											buffer, content.getLocation());
									final List<RiakObject<byte[]>> list = new ArrayList<RiakObject<byte[]>>();
									list.add(ro);
									handler.handle(new RestRiakResponse<List<RiakObject<byte[]>>>() {
										@Override
										public List<RiakObject<byte[]>> getContents() {
											return list;
										}
									});
									return;
								} else if (response.getStatus().getCode() == 300) {
									// TODO multipart ?
								}
								throw new IllegalStateException();
							}
						}));
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

	@Override
	public RiakFuture post(String bucket, RiakObject<byte[]> content,
			PutOptions options, RiakResponseHandler<RiakObject<byte[]>> handler) {
		// TODO Auto-generated method stub
		return null;
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
		return handle("delete", request, handler,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) throws Exception {
						if (receive instanceof HttpResponse) {
							HttpResponse response = (HttpResponse) receive;
							if (NettyUtil.isSuccessful(response.getStatus())) {
								handler.handle(new NoOpResponse());
								return true;
							}
						}
						throw new IllegalStateException();
					}
				});
	}

	@Override
	public RiakFuture delete(Location location, Quorum readWrite,
			RiakResponseHandler<_> handler) {
		notNull(location, "location");
		notNull(readWrite, "readWrite");

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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture mapReduce(String rawJson,
			RiakResponseHandler<MapReduceResponse> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture getStream(String key, GetOptions options,
			InputStreamHandler handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture putStream(RiakObject<OutputStreamHandler> content,
			RiakResponseHandler<String> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture walk(Location walkbegin, List<LinkCondition> conditions,
			RiakResponseHandler<List<RiakObject<byte[]>>> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture getStats(RiakResponseHandler<ObjectNode> handler) {
		// TODO Auto-generated method stub
		return null;
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

	abstract class RestRiakResponse<T> extends AbstractRiakResponse implements
			RiakContentsResponse<T> {
		@Override
		public void operationComplete() {
			complete();
		}
	}

	class RestRiakObjectResponse extends AbstractRiakObjectResponse {
		public RestRiakObjectResponse(RiakObject<byte[]> ro) {
			super(ro);
		}

		@Override
		public void operationComplete() {
			complete();
		}
	}

	class NoOpResponse extends RestRiakResponse<_> {
		@Override
		public _ getContents() {
			return _._;
		}
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
			complete();
		}
	}

	protected void complete() {
		this.support.complete();
	}
}
