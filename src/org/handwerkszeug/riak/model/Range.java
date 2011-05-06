package org.handwerkszeug.riak.model;

import static org.handwerkszeug.riak.util.Validation.notNull;

/**
 * @author taichi
 * @see <a href="http://www.ietf.org/rfc/rfc2616.txt">RFC2616 - Hypertext
 *      Transfer Protocol -- HTTP/1.1 14.35.1 Byte Ranges</a>
 */
public class Range {

	Long first;
	Long last;

	protected Range(Long first, Long last) {
		this.first = first;
		this.last = last;

	}

	public static Range range(long first, long last) {
		if (first < 0L) {
			throw new IllegalArgumentException("first");
		}
		if (last < 1L) {
			throw new IllegalArgumentException("last");
		}
		return new Range(first, last);
	}

	public static Range first(long first) {
		if (first < 0L) {
			throw new IllegalArgumentException("first");
		}
		return new Range(first, null);
	}

	public static Range last(long last) {
		if (last < 0L) {
			throw new IllegalArgumentException("last");
		}
		return new Range(null, last);
	}

	public static Range ranges(final Range... ranges) {
		notNull(ranges, "ranges");
		if (ranges.length < 1) {
			throw new IllegalArgumentException("ranges");
		}
		StringBuilder stb = new StringBuilder();
		stb.append(BytesUnit);
		for (int i = 0, l = ranges.length; i < l; i++) {
			stb.append(ranges[i].toByteRange());
			if (i + 1 < l) {
				stb.append(',');
			}
		}
		final String s = stb.toString();
		return new Range(null, null) {
			@Override
			public String toString() {
				return s;
			}
		};
	}

	public static final String BytesUnit = "bytes=";

	protected String toByteRange() {
		StringBuilder stb = new StringBuilder();
		if (first != null) {
			stb.append(String.valueOf(this.first));
		}
		if (last != null) {
			stb.append('-');
			stb.append(String.valueOf(this.last));
		}
		return stb.toString();
	}

	public String toRangeSpec() {
		return BytesUnit + toByteRange();
	}

	@Override
	public String toString() {
		return toRangeSpec();
	}
}
