package de.tiqsolutions.webhdfs;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

public class Handler extends URLStreamHandler {
	private final URLStreamHandler base;

	public Handler() {
		Configuration conf = new Configuration();
		conf.addResource("webhdfs-default.xml");
		this.base = new FsUrlStreamHandlerFactory(conf)
				.createURLStreamHandler("webhdfs");
	}

	@Override
	protected URLConnection openConnection(URL u) throws IOException {
		try {
			Method m = this.base.getClass().getDeclaredMethod("openConnection",
					URL.class);
			m.setAccessible(true);
			return (URLConnection) m.invoke(this.base, u);
		} catch (IllegalAccessException | IllegalArgumentException
				| NoSuchMethodException | SecurityException
				| InvocationTargetException e) {
			throw new IOException(e.getLocalizedMessage(), e);
		}
	}
}
