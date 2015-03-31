/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *******************************************************************************/
package de.tiqsolutions.hdfs;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

public class Handler extends URLStreamHandler {

	private final URLStreamHandler base;

	public Handler() {
		base = new FsUrlStreamHandlerFactory()
				.createURLStreamHandler(HadoopFileSystem.SCHEME);
	}

	@Override
	protected URLConnection openConnection(URL u) throws IOException {
		try {
			Method m = base.getClass().getDeclaredMethod("openConnection",
					URL.class);
			m.setAccessible(true);
			return (URLConnection) m.invoke(base, u);
		} catch (NoSuchMethodException | SecurityException
				| IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			throw new IOException(e.getLocalizedMessage(), e);
		}

	}

}
