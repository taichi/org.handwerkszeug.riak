package org.handwerkszeug.riak.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MessageLoader {

	protected static Logger logger = Logger.getLogger(MessageLoader.class
			.getName());

	public static void load(Class<?> holder) {
		load(holder, holder.getName(), holder.getClassLoader());
	}

	public static void load(Class<?> holder, String bundlename,
			ClassLoader loader) {
		ResourceBundle rb = getBundle(bundlename, loader);
		if (rb == null) {
			return;
		}
		for (Field field : holder.getDeclaredFields()) {
			if (validateMask(field) == false) {
				String key = field.getName();
				try {
					if (String.class.isAssignableFrom(field.getType())) {
						field.set(null, rb.getString(key));
					}
				} catch (MissingResourceException e) {
					notfound(key, bundlename);
				} catch (Exception e) {
					error(e);
				}
			}
		}
	}

	protected static boolean validateMask(Field f) {
		final int MOD_EXPECTED = Modifier.PUBLIC | Modifier.STATIC;
		final int MOD_MASK = MOD_EXPECTED | Modifier.FINAL;
		return (f.getModifiers() & MOD_MASK) != MOD_EXPECTED;
	}

	protected static ResourceBundle getBundle(String name, ClassLoader loader) {
		try {
			return ResourceBundle.getBundle(name, Locale.getDefault(), loader);
		} catch (MissingResourceException e) {
			error(e);
			return null;
		}
	}

	protected static void error(Throwable t) {
		logger.log(Level.ALL, t.getMessage(), t);
	}

	protected static void notfound(String key, String bundlename) {
		logger.log(Level.WARNING, key + " not found in " + bundlename);
	}
}
