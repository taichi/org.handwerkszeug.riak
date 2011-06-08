package org.handwerkszeug.riak.transport.rest.internal;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.codehaus.jackson.map.ObjectMapper;
import org.handwerkszeug.riak.Markers;
import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak.model.Bucket;
import org.handwerkszeug.riak.model.DefaultRiakObject;
import org.handwerkszeug.riak.model.GetOptions;
import org.handwerkszeug.riak.model.Link;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.PutOptions;
import org.handwerkszeug.riak.model.Quorum;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.nls.Messages;
import org.handwerkszeug.riak.transport.rest.InputStreamHandler;
import org.handwerkszeug.riak.transport.rest.LinkCondition;
import org.handwerkszeug.riak.transport.rest.RestRiakConfig;
import org.handwerkszeug.riak.transport.rest.RiakHttpHeaders;
import org.handwerkszeug.riak.util.HttpUtil;
import org.handwerkszeug.riak.util.StringUtil;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMessage;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.codec.http.QueryStringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author taichi
 */
public class RequestFactory {

	static final Logger LOG = LoggerFactory.getLogger(RequestFactory.class);

	RestRiakConfig config;
	String host;

	String clientId;

	ObjectMapper objectMapper = new ObjectMapper();

	public RequestFactory(String host, RestRiakConfig config) {
		super();
		this.host = removeSlashIfNeed(host);
		this.config = config;
	}

	protected String removeSlashIfNeed(String uri) {
		return uri.endsWith("/") ? uri.substring(0, uri.length() - 1) : uri;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public String getClientId() {
		return this.clientId;
	}

	public HttpRequest newRequest(String path, HttpMethod method) {
		return newRequest(this.host + "/" + this.config.getRawName(), path,
				method);
	}

	public HttpRequest newDeleteRequest(Location location) {
		HttpRequest request = newRequest("/" + location.getBucket() + "/"
				+ location.getKey(), HttpMethod.DELETE);
		return request;
	}

	public HttpRequest newGetBucketRequest(String bucket) {
		HttpRequest request = newRequest("/" + bucket + "?props=true",
				HttpMethod.GET);
		return request;
	}

	public HttpRequest newGetRequst(Location location) {
		HttpRequest request = newRequest("/" + location.getBucket() + "/"
				+ location.getKey(), HttpMethod.GET);
		return request;
	}

	public HttpRequest newGetRequst(Location location, GetOptions options) {
		HttpRequest request = newGetRequst(location);
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

	public HttpRequest newListBucketsRequest() {
		HttpRequest request = newRequest("?buckets=true", HttpMethod.GET);
		return request;
	}

	public HttpRequest newListKeysRequest(String bucket) {
		HttpRequest request = newRequest("/" + bucket
				+ "?props=false&keys=stream", HttpMethod.GET);
		return request;
	}

	public HttpRequest newPingRequest() {
		HttpRequest request = newRequest("/ping", HttpMethod.GET);
		return request;
	}

	public HttpRequest newGetStatsRequest() {
		HttpRequest request = newRequest(this.host, "/stats", HttpMethod.GET);
		return request;
	}

	public HttpRequest newPostRequest(RiakObject<byte[]> content) {
		HttpRequest request = newRequest("/"
				+ content.getLocation().getBucket(), HttpMethod.POST);
		merge(request, content);
		return request;
	}

	public HttpRequest newPutRequest(RiakObject<byte[]> content) {
		Location location = content.getLocation();
		HttpRequest request = newRequest("/" + location.getBucket() + "/"
				+ location.getKey(), HttpMethod.PUT);
		merge(request, content);

		return request;
	}

	public HttpRequest newPutRequest(RiakObject<byte[]> content,
			PutOptions options) {
		HttpRequest request = newPutRequest(content);

		QueryStringEncoder params = to(options, request);
		request.setUri(params.toString());
		return request;
	}

	public HttpRequest newPostRequest(RiakObject<byte[]> content,
			PutOptions options) {
		HttpRequest request = newPostRequest(content);
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

	public HttpRequest newDeleteRequest(Location location, Quorum readWrite) {
		HttpRequest request = newDeleteRequest(location);
		QueryStringEncoder params = new QueryStringEncoder(request.getUri());
		params.addParam("rw", readWrite.getString());
		request.setUri(params.toString());
		return request;
	}

	public HttpRequest newSetBucketRequest(Bucket bucket) {
		try {
			BucketHolder holder = new BucketHolder();
			holder.props = bucket;
			ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
			OutputStream out = new ChannelBufferOutputStream(buffer);
			this.objectMapper.writeValue(out, holder);
			HttpRequest request = newRequest("/" + bucket.getName(),
					HttpMethod.PUT);
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

	public HttpRequest newMapReduceRequest() {
		HttpRequest request = newRequest(this.host,
				"/" + this.config.getMapReduceName() + "?chunked=true",
				HttpMethod.POST);
		request.setHeader(HttpHeaders.Names.CONTENT_TYPE,
				RiakHttpHeaders.CONTENT_JSON);

		return request;
	}

	public HttpRequest newWalkRequst(Location walkbegin,
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
		return newRequest(stb.toString(), HttpMethod.GET);
	}

	public HttpRequest newRequest(String app, String path, HttpMethod method) {
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

	public void merge(HttpRequest request, RiakObject<byte[]> content) {
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

	public <T> void mergeHeaders(HttpRequest request, RiakObject<T> content) {
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

		if (content.getLinks() != null && content.getLinks().isEmpty() == false) {
			addLinkHeader(request, content);
		}

		if (content.getLastModified() != null) {
			request.setHeader(HttpHeaders.Names.LAST_MODIFIED,
					HttpUtil.format(content.getLastModified()));
		}

		if (content.getUserMetadata() != null
				&& content.getUserMetadata().isEmpty() == false) {
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
			stb.append(this.config.getRawName());
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

	public RiakObject<byte[]> convert(HttpMessage headers,
			ChannelBuffer buffer, Location location) {
		DefaultRiakObject ro = new DefaultRiakObject(location);
		ro.setContent(buffer.array());

		convertHeaders(headers, ro);

		return ro;
	}

	public <T> void convertHeaders(HttpMessage headers, RiakObject<T> ro) {
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

	public HttpRequest newGetStreamRequest(String key) {
		HttpRequest request = newRequest(this.host,
				"/" + this.config.getLuwakName() + "/" + key, HttpMethod.GET);
		return request;
	}

	public HttpRequest newStreamRequest(
			final RiakObject<InputStreamHandler> content, String key,
			HttpMethod method) {
		HttpRequest request = newRequest(this.host,
				"/" + this.config.getLuwakName() + "/" + key, method);

		mergeHeaders(request, content);
		request.setHeader(HttpHeaders.Names.EXPECT, HttpHeaders.Values.CONTINUE);
		request.setHeader(HttpHeaders.Names.CONTENT_LENGTH, content
				.getContent().getContentLength());
		return request;
	}

	public HttpRequest newDeleteRequest(String key) {
		HttpRequest request = newRequest(this.host,
				"/" + this.config.getLuwakName() + "/" + key, HttpMethod.DELETE);
		return request;
	}
}
