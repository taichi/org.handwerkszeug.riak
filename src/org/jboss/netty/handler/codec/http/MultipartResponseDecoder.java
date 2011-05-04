package org.jboss.netty.handler.codec.http;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

/**
 * @author taichi
 */
public class MultipartResponseDecoder extends SimpleChannelUpstreamHandler {

	String dashBoundary;
	String closeBoundary;

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
			} else if (response.getStatus().getCode() == 300
					&& setUpBoundary(response)) {
				ChannelBuffer buffer = response.getContent();
				response.setContent(ChannelBuffers.EMPTY_BUFFER);
				Channels.fireMessageReceived(ctx, response,
						e.getRemoteAddress());
				splitMultipart(ctx, e, buffer);
				return;
			}
		} else if (o instanceof HttpChunk) {
			HttpChunk chunk = (HttpChunk) o;
			if (this.dashBoundary != null) {
				if (chunk.isLast()) {
					this.dashBoundary = null;
					this.closeBoundary = null;
				} else {
					ChannelBuffer buffer = chunk.getContent();
					splitMultipart(ctx, e, buffer);
				}
				return;
			}
		}
		ctx.sendUpstream(e);
	}

	protected boolean setUpBoundary(HttpResponse response) {
		String b = getBoundary(response);
		if (b != null) {
			this.dashBoundary = "--" + b;
			this.closeBoundary = this.dashBoundary + "--";
			return true;
		}
		return false;
	}

	static final Pattern MultiPart = Pattern
			.compile(
					"multipart/mixed;\\s*boundary=(([\\w'\\(\\),-./:=\\?]{1,70})|\"([\\w'\\(\\),-./:=\\?]{1,70})\")",
					Pattern.CASE_INSENSITIVE);

	protected String getBoundary(HttpResponse response) {
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
		}
	}

	enum State {
		SKIP_CONTROL_CHARS, READ_BOUNDARY, READ_HEADERS, READ_CONTENT, EPILOGUE;
	}

	protected PartMessage parse(ChannelBuffer buffer) {
		DefaultPartMessage multipart = new DefaultPartMessage();
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
				State[] next = new State[1];
				int length = seekNextBoundary(buffer, next);
				multipart.setContent(buffer.readBytes(length));
				state = next[0];
				return multipart;
			}
			case EPILOGUE: {
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

	private State readBoundary(ChannelBuffer buffer) {
		String line = readLine(buffer);
		if (this.dashBoundary.equals(line)) {
			return State.READ_HEADERS;
		} else if (line.equals(this.closeBoundary)) {
			return State.EPILOGUE;
		}
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

	private int seekNextBoundary(ChannelBuffer buffer, State[] next) {
		int readerIndex = buffer.readerIndex();
		int length = 0;
		String line = "";
		while (buffer.readable()) {
			line = readLine(buffer);
			if (line.isEmpty()) {
				next[0] = State.EPILOGUE;
				break;
			} else if (this.dashBoundary.equals(line)) {
				length -= this.dashBoundary.length();
				length -= 4; // CRLF x 2
				next[0] = State.READ_BOUNDARY;
				break;
			} else if (this.closeBoundary.equals(line)) {
				length -= this.closeBoundary.length();
				length -= 4;
				next[0] = State.READ_BOUNDARY;
				break;
			}
		}

		length += (buffer.readerIndex() - readerIndex);
		buffer.readerIndex(readerIndex);
		return length;
	}
}
