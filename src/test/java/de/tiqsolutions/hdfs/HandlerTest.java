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
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;

import org.junit.Assert;
import org.junit.Test;

public class HandlerTest extends HadoopTestBase {
	public HandlerTest(String base) {
		super(base);
	}

	@Test
	public void testHdfsURL() throws IOException, URISyntaxException {
		URL url = BASE_URI.resolve("/test.csv").toURL();
		URLConnection conn = url.openConnection();
		Assert.assertNotNull((Object) conn);
		Assert.assertEquals((Object) "org.apache.hadoop.fs.FsUrlConnection",
				(Object) conn.getClass().getName());
		try (InputStream in = conn.getInputStream()) {
			Assert.assertNotNull((Object) in);
		}
	}

	// @Test
	// public void testWebhdfsURL() throws IOException, URISyntaxException {
	// URL url = WEBHDFS_BASE_URI.resolve("/test.csv").toURL();
	// URLConnection conn = url.openConnection();
	// Assert.assertNotNull((Object) conn);
	// Assert.assertEquals((Object) "org.apache.hadoop.fs.FsUrlConnection",
	// (Object) conn.getClass().getName());
	// try (InputStream in = conn.getInputStream()) {
	// Assert.assertNotNull((Object) in);
	// }
	// }
}
