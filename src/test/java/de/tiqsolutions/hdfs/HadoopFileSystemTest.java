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
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.Arrays;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HadoopFileSystemTest extends HadoopTestBase {
	private URI hdfsfile;

	@Before
	public void setUpBefore() throws Exception {
		this.hdfsfile = HDFS_BASE_URI.resolve("/test.csv");
	}

	@Test
	public void testClose() throws IOException {
		FileSystem fs = null;
		try (FileSystem f = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			fs = f;
			Assert.assertTrue((boolean) fs.isOpen());
		}
		Assert.assertFalse((boolean) fs.isOpen());
	}

	@Test
	public void testIsOpen() throws IOException {
		this.testClose();
	}

	@Test
	public void testIsReadOnly() throws IOException {
		try (FileSystem f = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			Assert.assertFalse((boolean) f.isReadOnly());
		}
	}

	@Test
	public void testHadoopFileSystem() throws IOException {
		try (FileSystem f = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
		}
	}

	@Test
	public void testGetFileContext() throws IOException {
		try (FileSystem f = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			((HadoopFileSystem) f).getFileContext().getDefaultFileSystem()
					.equals((Object) f);
		}
	}

	@Test
	public void testGetSeparator() throws IOException {
		try (FileSystem f = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			Assert.assertEquals((Object) f.getSeparator(), (Object) "/");

		}
	}

	@Test
	public void testGetRootDirectories() throws IOException {
		try (FileSystem f = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			int i = 0;
			for (Path p : f.getRootDirectories()) {
				Assert.assertEquals((Object) f.getPath(null, new String[0]),
						(Object) p);
				++i;
			}
			Assert.assertEquals((long) 1, (long) i);
		}
	}

	@Test
	public void testGetFileStores() throws IOException {
		try (FileSystem f = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			f.getFileStores();

		}
	}

	@Test
	public void testSupportedFileAttributeViews() throws IOException {
		try (FileSystem f = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {

			Set<String> fav = f.supportedFileAttributeViews();
			Assert.assertTrue((boolean) fav.containsAll(Arrays.asList("basic",
					"hdfs")));
		}
	}

	@Test
	public void testGetPathStringStringArray() throws IOException {
		try (FileSystem f = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			Assert.assertEquals(
					(Object) f.getPath("/test/test.csv", new String[0]),
					(Object) f.getPath("/", "test/", "test.csv"));
		}
	}

	@Test
	public void testGetPathMatcherString() throws IOException {
		try (FileSystem f = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			PathMatcher matcher = f.getPathMatcher("glob:*test*");
			Assert.assertTrue((boolean) matcher.matches(f.getPath("/test",
					new String[0])));
			Assert.assertTrue((boolean) matcher.matches(f.getPath("test",
					new String[0])));
			Assert.assertFalse((boolean) matcher.matches(f.getPath("etst",
					new String[0])));
		}
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testGetUserPrincipalLookupService() throws IOException {
		try (FileSystem f = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			UserPrincipalLookupService up = f.getUserPrincipalLookupService();
			String user = "test";
			UserPrincipal principal = up.lookupPrincipalByName(user);
			Assert.assertEquals((Object) user, (Object) principal.getName());
			String group = "test";
			GroupPrincipal groupPrincipal = up
					.lookupPrincipalByGroupName(group);
			Assert.assertEquals((Object) group,
					(Object) groupPrincipal.getName());
		}
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testNewWatchService() throws IOException {
		try (FileSystem f = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			f.newWatchService();

		}
	}
}
