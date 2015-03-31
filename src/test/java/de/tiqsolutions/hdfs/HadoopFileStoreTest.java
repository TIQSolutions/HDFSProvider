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
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.DosFileAttributeView;
import java.nio.file.attribute.FileAttributeView;
import java.util.Iterator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HadoopFileStoreTest extends HadoopTestBase {
	private FileStore fileStore;
	private FileSystem fs;

	@Before
	public void acuireFileStore() throws IOException {
		this.fs = FileSystems.newFileSystem(HDFS_BASE_URI, System.getenv());
		Iterator<FileStore> iterator = this.fs.getFileStores().iterator();
		if (iterator.hasNext()) {
			FileStore fileStore = iterator.next();
			this.fileStore = (HadoopFileStore) fileStore;
		}
	}

	@After
	public void releaseFileSystem() throws IOException {
		this.fs.close();
	}

	@Test
	public void testIsReadOnly() {
		Assert.assertFalse((boolean) this.fileStore.isReadOnly());
	}

	@Test
	public void testGetTotalSpace() throws IOException {
		Assert.assertTrue((boolean) (this.fileStore.getTotalSpace() > 0));
	}

	@Test
	public void testGetUsableSpace() throws IOException {
		Assert.assertTrue((boolean) (this.fileStore.getUsableSpace() > 0));
	}

	@Test
	public void testGetUnallocatedSpace() throws IOException {
		Assert.assertTrue((boolean) (this.fileStore.getUnallocatedSpace() > 0));
	}

	@Test
	public void testName() {
		Assert.assertEquals((Object) ((HadoopFileStore) this.fileStore)
				.getFileSystem().getFileContext().getDefaultFileSystem()
				.getUri().toString(), (Object) this.fileStore.name());
	}

	@Test
	public void testType() {
		Assert.assertEquals((Object) "hdfs", (Object) this.fileStore.type());
	}

	@Test
	public void testSupportsFileAttributeViewClassOfQextendsFileAttributeView() {
		Assert.assertTrue((boolean) this.fileStore
				.supportsFileAttributeView(BasicFileAttributeView.class));
		Assert.assertTrue((boolean) this.fileStore
				.supportsFileAttributeView(HadoopFileAttributeView.class));
		Assert.assertFalse((boolean) this.fileStore
				.supportsFileAttributeView(FileAttributeView.class));
		Assert.assertFalse((boolean) this.fileStore
				.supportsFileAttributeView(DosFileAttributeView.class));
	}

	@Test
	public void testSupportsFileAttributeViewString() {
		Assert.assertTrue((boolean) this.fileStore
				.supportsFileAttributeView("basic"));
		Assert.assertTrue((boolean) this.fileStore
				.supportsFileAttributeView("hdfs"));
		Assert.assertTrue((boolean) this.fileStore
				.supportsFileAttributeView("posix"));
	}

	@Test
	public void testGetFileStoreAttributeViewClassOfV() {
		Assert.assertTrue((boolean) HadoopFileStoreAttributeView.class.isInstance(this.fileStore
				.getFileStoreAttributeView(HadoopFileStoreAttributeView.class)));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testGetAttributeString() throws IOException {
		try {
			Assert.assertEquals((Object) this.fileStore.getTotalSpace(),
					(Object) this.fileStore.getAttribute("hdfs:totalSpace"));
			Assert.assertEquals((Object) this.fileStore.getUsableSpace(),
					(Object) this.fileStore.getAttribute("hdfs:usableSpace"));
			Assert.assertEquals((Object) this.fileStore.getUnallocatedSpace(),
					(Object) this.fileStore
							.getAttribute("hdfs:unallocatedSpace"));
		} catch (UnsupportedOperationException e) {
			throw new IllegalArgumentException(e.getLocalizedMessage(), e);
		}
		this.fileStore.getAttribute("totalSpace");
	}
}
