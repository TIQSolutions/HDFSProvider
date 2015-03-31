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
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

import org.apache.hadoop.fs.FileStatus;

class BasicFileAttributesImpl implements BasicFileAttributes {

	protected final HadoopFileSystemPath path;
	protected final FileStatus fileStatus;

	public BasicFileAttributesImpl(HadoopFileSystemPath path)
			throws IOException {
		this.path = path;
		fileStatus = ((HadoopFileSystem) path.getFileSystem()).getFileContext()
				.getFileStatus(path.getPath());
	}

	@Override
	public FileTime lastModifiedTime() {
		return FileTime.fromMillis(fileStatus.getModificationTime());
	}

	@Override
	public FileTime lastAccessTime() {
		return FileTime.fromMillis(fileStatus.getAccessTime());
	}

	@Override
	public FileTime creationTime() {
		return lastModifiedTime();
	}

	@Override
	public boolean isRegularFile() {
		return fileStatus.isFile();
	}

	@Override
	public boolean isDirectory() {
		return fileStatus.isDirectory();
	}

	@Override
	public boolean isSymbolicLink() {
		return fileStatus.isSymlink();
	}

	@Override
	public boolean isOther() {
		return !isRegularFile() && !isDirectory() && !isSymbolicLink();
	}

	@Override
	public long size() {
		return fileStatus.getLen();
	}

	@Override
	public Object fileKey() {
		return fileStatus.hashCode();
	}

}
