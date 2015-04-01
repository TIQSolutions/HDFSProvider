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
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.util.ArrayList;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class HadoopFileSystemPathTest extends HadoopTestBase {
	private FileSystem fs;
	private URI hdfsfile;

	public HadoopFileSystemPathTest(String base) throws IOException {
		super(base);
		this.fs = FileSystems.newFileSystem(BASE_URI, System.getenv());
		this.hdfsfile = BASE_URI.resolve("/test.csv");
	}

	@After
	public void releaseFileSystem() throws IOException {
		this.fs.close();
	}

	@Test
	public void testHadoopFileSystemPath() {
		Path p = this.fs.getPath("/test.csv", new String[0]);
		Assert.assertEquals((Object) p.toUri().getPath(),
				(Object) this.hdfsfile.getPath());
	}

	@Test
	public void testGetFileSystem() {
		Path p = this.fs.getPath(null, new String[0]);
		Assert.assertEquals((Object) this.fs, (Object) p.getFileSystem());
	}

	@Test
	public void testIsAbsolute() {
		Path p = this.fs.getPath(null, new String[0]);
		Assert.assertTrue((boolean) p.toAbsolutePath().isAbsolute());
	}

	@Test
	public void testGetRoot() {
		Path p = this.fs.getPath(null, new String[0]);
		Assert.assertEquals((Object) p.toAbsolutePath().getRoot(),
				(Object) p.getRoot());
	}

	@Test
	public void testGetFileName() {
		Path p = this.fs.getPath("/test.csv", new String[0]);
		Assert.assertEquals((Object) "test.csv", (Object) p.getFileName()
				.toString());
	}

	@Test
	public void testGetParent() {
		Assert.assertEquals((Object) this.fs
				.getPath("/test.csv", new String[0]).getParent(),
				(Object) this.fs.getPath("/", new String[0]));
	}

	@Test
	public void testGetNameCount() {
		Assert.assertEquals((long) 2,
				(long) this.fs.getPath("/test/", "test.csv").getNameCount());
	}

	@Test
	public void testGetName() {
		Assert.assertEquals((Object) this.fs.getPath("test.csv", new String[0])
				.getFileName(), (Object) this.fs.getPath("/test/", "test.csv")
				.getName(1));
		Assert.assertEquals((Object) this.fs.getPath("test/", new String[0])
				.getFileName(), (Object) this.fs.getPath("/test/", "test.csv")
				.getName(0));
		Assert.assertNull((Object) this.fs.getPath("/test/", "test.csv")
				.getName(2));
	}

	@Test
	public void testSubpath() {
		Assert.assertEquals(
				(Object) this.fs.getPath("test1/test2/", new String[0]),
				(Object) this.fs.getPath("/test1/test2/", "test3.csv").subpath(
						0, 2));
	}

	@Test
	public void testStartsWithPath() {
		Assert.assertTrue((boolean) this.fs.getPath("/test/test.csv",
				new String[0]).startsWith(
				this.fs.getPath("/test/", new String[0])));
	}

	@Test
	public void testStartsWithString() {
		Assert.assertTrue((boolean) this.fs.getPath("/test/test.csv",
				new String[0]).startsWith("/test/"));
	}

	@Test
	public void testEndsWithPath() {
		Assert.assertTrue((boolean) this.fs.getPath("/test/test.csv",
				new String[0]).endsWith(
				this.fs.getPath("test.csv", new String[0])));
	}

	@Test
	public void testEndsWithString() {
		Assert.assertTrue((boolean) this.fs.getPath("/test/test.csv",
				new String[0]).endsWith("test.csv"));
	}

	@Test
	public void testNormalize() {
		Assert.assertEquals(
				(Object) this.fs.getPath("/test/test.csv", new String[0]),
				(Object) this.fs.getPath("/test/test/../test.csv",
						new String[0]).normalize());
	}

	@Test
	public void testResolvePath() {
		Assert.assertEquals(
				(Object) this.fs.getPath("/test/test.csv", new String[0]),
				(Object) this.fs.getPath("/test/", new String[0]).resolve(
						this.fs.getPath("test.csv", new String[0])));
	}

	@Test
	public void testResolveString() {
		Assert.assertEquals(
				(Object) this.fs.getPath("/test/test.csv", new String[0]),
				(Object) this.fs.getPath("/test/", new String[0]).resolve(
						"test.csv"));
	}

	@Test
	public void testResolveSiblingPath() {
		Assert.assertEquals(
				(Object) this.fs.getPath("/test/test.csv", new String[0]),
				(Object) this.fs.getPath("/test/test.csv", new String[0])
						.resolveSibling(
								this.fs.getPath("test.csv", new String[0])));
	}

	@Test
	public void testResolveSiblingString() {
		Assert.assertEquals((Object) this.fs.getPath("/test/test.csv",
				new String[0]),
				(Object) this.fs.getPath("/test/test.csv", new String[0])
						.resolveSibling("test.csv"));
	}

	@Test
	public void testRelativize() {
		Assert.assertEquals(
				(Object) this.fs.getPath("/test.csv", new String[0]),
				(Object) this.fs.getPath("/test", new String[0]).relativize(
						this.fs.getPath("/test", "test.csv")));
	}

	@Test
	public void testToUri() throws URISyntaxException {
		Assert.assertEquals((Object) this.fs.getPath("/test/", "test.csv")
				.toAbsolutePath().toUri(),
				(Object) BASE_URI.resolve("/test/test.csv"));
	}

	@Test
	public void testToAbsolutePath() {
		Assert.assertEquals((Object) this.fs.getPath("/test/", "test.csv")
				.toAbsolutePath(),
				(Object) this.fs.getPath("/test/", "test/", "../", "test.csv")
						.toAbsolutePath());
	}

	@Test
	public void testToRealPath() throws IOException {
		Assert.assertEquals((Object) this.fs.getPath("/test/test.csv",
				new String[0]),
				(Object) this.fs.getPath("/test/test.csv", new String[0])
						.toRealPath(LinkOption.NOFOLLOW_LINKS));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testToFile() {
		Path p = this.fs.getPath("/test", "test.csv");
		p.toFile();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testRegisterWatchServiceKindOfQArrayModifierArray()
			throws IOException {
		this.fs.getPath(null, new String[0]).register(null,
				new WatchEvent.Kind[] { null });
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testRegisterWatchServiceKindOfQArray() throws IOException {
		this.fs.getPath(null, new String[0]).register(null,
				new WatchEvent.Kind[0], new WatchEvent.Modifier[] { null });
	}

	@Test
	public void testIterator() {
		Path p = this.fs.getPath("/test/", "test/", "test.csv");
		ArrayList<String> segments = new ArrayList<String>();
		for (Path path : p) {
			segments.add(path.toUri().toString());
		}
		Assert.assertEquals((long) 3, (long) segments.size());
		Assert.assertArrayEquals((Object[]) new String[] { "test/", "test/",
				"test.csv" }, (Object[]) segments.toArray());
	}

	@Test
	public void testCompareTo() {
		Assert.assertTrue((boolean) (this.fs.getPath("/test/test.csv",
				new String[0]).compareTo(
				this.fs.getPath("/test", new String[0])) > 0));
	}
}
