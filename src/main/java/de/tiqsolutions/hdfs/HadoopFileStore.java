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

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;

public class HadoopFileStore extends FileStore implements Closeable {
	private final HadoopFileSystem fileSystem;

	public HadoopFileStore(HadoopFileSystem fileSystem) {
		this.fileSystem = fileSystem;
	}

	@Override
	public String name() {
		return fileSystem.getFileContext().getDefaultFileSystem().getUri()
				.toString();
	}

	@Override
	public String type() {
		return HadoopFileSystem.SCHEME;
	}

	@Override
	public boolean isReadOnly() {
		return fileSystem.isReadOnly();
	}

	@Override
	public long getTotalSpace() throws IOException {
		return fileSystem.getFileContext().getDefaultFileSystem().getFsStatus()
				.getCapacity();
	}

	@Override
	public long getUsableSpace() throws IOException {
		return fileSystem.getFileContext().getDefaultFileSystem().getFsStatus()
				.getRemaining();
	}

	@Override
	public long getUnallocatedSpace() throws IOException {
		// fileSystem.getFileContext().getDefaultFileSystem().getStatistics().getBytesRead()
		return fileSystem.getFileContext().getDefaultFileSystem().getFsStatus()
				.getRemaining();
	}

	@Override
	public boolean supportsFileAttributeView(
			Class<? extends FileAttributeView> type) {
		return BasicFileAttributeView.class.equals(type)
				|| PosixFileAttributeView.class.equals(type)
				|| HadoopFileAttributeView.class.equals(type);
	}

	@Override
	public boolean supportsFileAttributeView(String name) {
		return BasicFileAttributeViewImpl.NAME.equals(name)
				|| PosixFileAttributeViewImpl.NAME.equals(name)
				|| HadoopFileAttributeViewImpl.NAME.equals(name);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V extends FileStoreAttributeView> V getFileStoreAttributeView(
			Class<V> type) {
		if (HadoopFileStoreAttributeView.class.isAssignableFrom(type))
			return (V) new HadoopFileStoreAttributeView() {
				@Override
				public String name() {
					return HadoopFileStore.this.name();
				}
			};
		return null;
	}

	@Override
	public Object getAttribute(String attribute) throws IOException {
		if (attribute.equals(type() + ":totalSpace"))
			return getTotalSpace();
		if (attribute.equals(type() + ":usableSpace"))
			return getUsableSpace();
		if (attribute.equals(type() + ":unallocatedSpace"))
			return getUnallocatedSpace();
		throw new UnsupportedOperationException();
	}

	public HadoopFileSystem getFileSystem() {
		return fileSystem;
	}

	@Override
	public void close() throws IOException {
		fileSystem.close();

	}

}
