package org.jboss.netty.handler.codec.http;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.handwerkszeug.riak.Markers;
import org.handwerkszeug.riak.util.StringUtil;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author taichi
 */
public class MultipartResponseDecoder extends SimpleChannelUpstreamHandler {

	static final Logger LOG = LoggerFactory
			.getLogger(MultipartResponseDecoder.class);

	String dashBoundary;
	String closeBoundary;

	State state;
	ContentRange contentRange;

	public MultipartResponseDecoder() {
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		Object o = e.getMessage();
		if (o instanceof HttpResponse) {
			this.dashBoundary = null;
			this.closeBoundary = null;
			HttpResponse response = (HttpResponse) o;
			if (response.isChunked()) {
				setUpBoundary(response);
			} else if (setUpBoundary(response)) {
				ChannelBuffer buffer = response.getContent().copy();

				response.setContent(ChannelBuffers.EMPTY_BUFFER);
				Channels.fireMessageReceived(ctx, response,
						e.getRemoteAddress());
				splitMultipart(ctx, e, buffer);
				return;
			}
		} else if (o instanceof HttpChunk) {
			HttpChunk chunk = (HttpChunk) o;

			if (this.contentRange != null
					&& State.READ_CHUNKD_CONTENT.equals(state)) {
				if (this.contentRange.pass(chunk.getContent())) {
					ctx.sendUpstream(e);
					return;
				} else {
					this.contentRange = null;
					this.state = State.SKIP_CONTROL_CHARS;
				}
			}
			if (this.dashBoundary != null) {
				if (chunk.isLast()) {
					clearBoudndary();
				} else {
					ChannelBuffer buffer = chunk.getContent();
					if (2 < buffer.readableBytes()) {
						splitMultipart(ctx, e, buffer);
					}
				}
				return;
			}
		}
		ctx.sendUpstream(e);
	}

	public boolean setUpBoundary(HttpMessage response) {
		String b = getBoundary(response);
		if (b != null) {
			this.dashBoundary = "--" + b;
			this.closeBoundary = this.dashBoundary + "--";
			state = State.SKIP_CONTROL_CHARS;
			return true;
		}
		return false;
	}

	public void clearBoudndary() {
		this.dashBoundary = null;
		this.closeBoundary = null;
	}

	static final Pattern MultiPart = Pattern
			.compile(
					"multipart/[a-z]+;\\s*boundary=(([\\w'\\(\\),-./:=\\?]{1,70})|\"([\\w'\\(\\),-./:=\\?]{1,70})\")",
					Pattern.CASE_INSENSITIVE);

	protected String getBoundary(HttpMessage response) {
		String type = response.getHeader(HttpHeaders.Names.CONTENT_TYPE);
		if (type != null && type.isEmpty() == false) {
			Matcher m = MultiPart.matcher(type);
			String b = null;
			if (m.find()) {
				b = m.group(2);
				if (b == null) {
					b = m.group(3);
				}
			}
			return b;
		}
		return null;
	}

	protected void splitMultipart(ChannelHandlerContext ctx, MessageEvent e,
			ChannelBuffer buffer) {
		while (buffer.readable()) {
			PartMessage msg = parse(buffer);
			Channels.fireMessageReceived(ctx, msg, e.getRemoteAddress());
			if (State.READ_CHUNKD_CONTENT.equals(state)) {
				break;
			}
		}
	}

	enum State {
		SKIP_CONTROL_CHARS, READ_BOUNDARY, READ_HEADERS, READ_CONTENT, READ_CHUNKD_CONTENT, EPILOGUE;
	}

	public PartMessage parse(ChannelBuffer buffer) {
		DefaultPartMessage multipart = new DefaultPartMessage();
		for (;;) {
			switch (state) {
			case SKIP_CONTROL_CHARS: {
				skipControlCharacters(buffer);
				state = State.READ_BOUNDARY;
				break;
			}
			case READ_BOUNDARY: {
				state = readBoundary(buffer);
				break;
			}
			case READ_HEADERS: {
				readHeaders(buffer, multipart);
				break;
			}
			case READ_CHUNKD_CONTENT: {
				return multipart;
			}
			case READ_CONTENT: {
				int length = seekNextBoundary(buffer);
				multipart.setContent(buffer.readBytes(length));
				return multipart;
			}
			case EPILOGUE: {
				multipart.setLast(true);
				state = State.SKIP_CONTROL_CHARS;
				return multipart;
			}
			default: {
				throw new IllegalStateException("Unknown state " + state);
			}
			}
		}
	}

	private void skipControlCharacters(ChannelBuffer buffer) {
		while (buffer.readable()) {
			char c = (char) buffer.readUnsignedByte();
			if (Character.isISOControl(c) == false
					&& Character.isWhitespace(c) == false) {
				buffer.readerIndex(buffer.readerIndex() - 1);
				break;
			}
		}
	}

	private State readBoundary(ChannelBuffer buffer) {
		String line = readLine(buffer);
		if (this.dashBoundary.equals(line)) {
			return State.READ_HEADERS;
		} else if (line.equals(this.closeBoundary)) {
			return State.EPILOGUE;
		}
		LOG.debug(Markers.DETAIL, line);
		LOG.debug(Markers.DETAIL, this.dashBoundary);
		throw new UnknownBoundaryException(line);
	}

	private void readHeaders(ChannelBuffer buffer, DefaultPartMessage message) {
		String line = readLine(buffer);
		String name = null;
		String value = null;
		do {
			char firstChar = line.charAt(0);
			if (name != null && (firstChar == ' ' || firstChar == '\t')) {
				value = value + ' ' + line.trim();
			} else {
				if (name != null) {
					message.addHeader(name, value);
				}
				String[] header = splitHeader(line);
				name = header[0];
				value = header[1];
			}

			line = readLine(buffer);
		} while (line.isEmpty() == false && buffer.readable());

		if (name != null) {
			message.addHeader(name, value);
		}
		String rangeHeader = message.getHeader(HttpHeaders.Names.CONTENT_RANGE);
		if (StringUtil.isEmpty(rangeHeader)) {
			state = State.READ_CONTENT;
		} else {
			ContentRange cr = parseRange(rangeHeader);
			if (cr == null) {
				state = State.READ_CONTENT;
			} else {
				this.contentRange = cr;
				state = State.READ_CHUNKD_CONTENT;
			}
		}
	}

	static final Pattern ContentRange = Pattern.compile(
			"\\s*bytes\\s*([0-9]+)-([0-9]+)/[0-9]+", Pattern.CASE_INSENSITIVE);

	protected ContentRange parseRange(String contentRange) {
		Matcher m = ContentRange.matcher(contentRange);
		if (m.find()) {
			String begin = m.group(1);
			String end = m.group(2);
			if (StringUtil.isEmpty(begin) == false
					&& StringUtil.isEmpty(end) == false) {
				ContentRange cr = new ContentRange();
				cr.length = Long.parseLong(end) - Long.parseLong(begin) + 1;
				return cr;
			}
		}
		return null;
	}

	class ContentRange {
		long length = 0L;
		long consumed = 0L;

		boolean pass(ChannelBuffer buffer) {
			consumed += buffer.readableBytes();
			return consumed <= length;
		}
	}

	private String readLine(ChannelBuffer buffer) {
		StringBuilder sb = new StringBuilder(64);
		while (buffer.readable()) {
			byte nextByte = buffer.readByte();
			if (nextByte == HttpCodecUtil.CR) {
				nextByte = buffer.readByte();
				if (nextByte == HttpCodecUtil.LF) {
					return sb.toString();
				}
			} else if (nextByte == HttpCodecUtil.LF) {
				return sb.toString();
			} else {
				sb.append((char) nextByte);
			}
		}
		return "";
	}

	private String[] splitHeader(String sb) {
		final int length = sb.length();
		int nameStart;
		int nameEnd;
		int colonEnd;
		int valueStart;
		int valueEnd;

		nameStart = findNonWhitespace(sb, 0);
		for (nameEnd = nameStart; nameEnd < length; nameEnd++) {
			char ch = sb.charAt(nameEnd);
			if (ch == ':' || Character.isWhitespace(ch)) {
				break;
			}
		}

		for (colonEnd = nameEnd; colonEnd < length; colonEnd++) {
			if (sb.charAt(colonEnd) == ':') {
				colonEnd++;
				break;
			}
		}

		valueStart = findNonWhitespace(sb, colonEnd);
		if (valueStart == length) {
			return new String[] { sb.substring(nameStart, nameEnd), "" };
		}

		valueEnd = findEndOfString(sb);
		return new String[] { sb.substring(nameStart, nameEnd),
				sb.substring(valueStart, valueEnd) };
	}

	private int findNonWhitespace(String sb, int offset) {
		int result;
		for (result = offset; result < sb.length(); result++) {
			if (!Character.isWhitespace(sb.charAt(result))) {
				break;
			}
		}
		return result;
	}

	private int findEndOfString(String sb) {
		int result;
		for (result = sb.length(); result > 0; result--) {
			if (!Character.isWhitespace(sb.charAt(result - 1))) {
				break;
			}
		}
		return result;
	}

	private int seekNextBoundary(ChannelBuffer buffer) {
		int readerIndex = buffer.readerIndex();
		int length = 0;
		String line = "";
		while (buffer.readable()) {
			line = readLine(buffer);
			if (line.isEmpty() && buffer.readable() == false) {
				state = State.EPILOGUE;
				break;
			} else if (this.dashBoundary.equals(line)) {
				length -= this.dashBoundary.length();
				length -= 4; // CRLF x 2
				state = State.SKIP_CONTROL_CHARS;
				break;
			} else if (this.closeBoundary.equals(line)) {
				length -= this.closeBoundary.length();
				length -= 4;
				state = State.SKIP_CONTROL_CHARS;
				break;
			}
		}

		length += (buffer.readerIndex() - readerIndex);

		if (LOG.isDebugEnabled()) {
			LOG.debug(Markers.DETAIL, "content length :" + length + " " + state);
		}

		buffer.readerIndex(readerIndex);
		return length;
	}
}
