package org.jboss.netty.handler.codec.http;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

/**
 * @author taichi
 */
public class MultipartChunkedResponseDecoder extends
		SimpleChannelUpstreamHandler {

	String boundary;

	static final Pattern MultiPart = Pattern.compile(
			"multipart/mixed;\\s*boundary=([\\w'\\(\\),-./:=\\?]{1,70})",
			Pattern.CASE_INSENSITIVE);

	public MultipartChunkedResponseDecoder() {
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		Object o = e.getMessage();
		if (o instanceof HttpResponse) {
			this.boundary = null;
			HttpResponse response = (HttpResponse) o;
			if (response.isChunked()) {
				String b = getBoundary(response);
				if (b != null) {
					this.boundary = b;
				}
			}
		} else if (o instanceof HttpChunk) {
			HttpChunk chunk = (HttpChunk) o;
			if (this.boundary != null) {
				if (chunk.isLast()) {
					this.boundary = null;
				} else {
					MultiPartChunk mpc = to(chunk);
					Channels.fireMessageReceived(ctx, mpc, e.getRemoteAddress());
				}
				return;
			}
		}
		ctx.sendUpstream(e);
	}

	protected String getBoundary(HttpResponse response) {
		String type = response.getHeader(HttpHeaders.Names.CONTENT_TYPE);
		if (type != null && type.isEmpty() == false) {
			Matcher m = MultiPart.matcher(type);
			String b = null;
			if (m.find()) {
				b = "--" + m.group(1);
			}
			return b;
		}
		return null;
	}

	protected MultiPartChunk to(HttpChunk chunk) {
		DefaultMultiPartChunk multipart = new DefaultMultiPartChunk();
		ChannelBuffer buffer = chunk.getContent();
		State state = State.SKIP_CONTROL_CHARS;
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
				state = State.READ_CONTENT;
				break;
			}
			case READ_CONTENT: {
				multipart.setContent(buffer.readBytes(buffer.readableBytes()));
				return multipart;
			}
			case END: {
				multipart.setLast(true);
				return multipart;
			}
			default: {
				throw new IllegalStateException("Unknown state " + state);
			}
			}
		}
	}

	private void skipControlCharacters(ChannelBuffer buffer) {
		for (;;) {
			char c = (char) buffer.readUnsignedByte();
			if (!Character.isISOControl(c) && !Character.isWhitespace(c)) {
				buffer.readerIndex(buffer.readerIndex() - 1);
				break;
			}
		}
	}

	enum State {
		SKIP_CONTROL_CHARS, READ_BOUNDARY, READ_HEADERS, READ_CONTENT, END;
	}

	private State readBoundary(ChannelBuffer buffer) {
		String line = readLine(buffer);
		if (this.boundary.equals(line)) {
			return State.READ_HEADERS;
		} else if (line.equals(this.boundary + "--")) {
			return State.END;
		}
		throw new IllegalStateException("Unknown Boundary");
	}

	private void readHeaders(ChannelBuffer buffer, DefaultMultiPartChunk chunk) {
		String line = readLine(buffer);
		String name = null;
		String value = null;
		do {
			char firstChar = line.charAt(0);
			if (name != null && (firstChar == ' ' || firstChar == '\t')) {
				value = value + ' ' + line.trim();
			} else {
				if (name != null) {
					chunk.addHeader(name, value);
				}
				String[] header = splitHeader(line);
				name = header[0];
				value = header[1];
			}

			line = readLine(buffer);
		} while (line.isEmpty() == false && buffer.readable());

		if (name != null) {
			chunk.addHeader(name, value);
		}
	}

	private String readLine(ChannelBuffer buffer) {
		StringBuilder sb = new StringBuilder(64);
		int readerIndex = buffer.readerIndex();
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
		buffer.readerIndex(readerIndex);
		throw new IllegalStateException("NotEnough");
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
}
