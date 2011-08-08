package org.handwerkszeug.riak.util;

import java.util.regex.Pattern;

/**
 * @author taichi
 */
public class Validation {

	static final Pattern isPositiveNumber = Pattern.compile("\\d+");

	public static void notNull(Object o, String name) {
		if (o == null) {
			throw new IllegalArgumentException(name);
		}
	}

	public static boolean isPositiveNumber(String s) {
		return s != null && isPositiveNumber.matcher(s).matches();
	}

	public static void positiveNumber(int i, String name) {
		if (i < 0) {
			throw new IllegalArgumentException(name);
		}
	}
}
