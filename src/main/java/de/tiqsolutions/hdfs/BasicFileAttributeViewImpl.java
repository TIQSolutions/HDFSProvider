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
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.NullArgumentException;

class BasicFileAttributeViewImpl implements BasicFileAttributeView {

	static final String NAME = "basic";
	protected final HadoopFileSystemPath path;

	public BasicFileAttributeViewImpl(Path path) {
		if (path == null)
			throw new NullArgumentException("path");
		if (!HadoopFileSystemPath.class.isInstance(path))
			throw new IllegalArgumentException("path");
		this.path = (HadoopFileSystemPath) path;

	}

	@Override
	public String name() {
		return NAME;
	}

	@Override
	public BasicFileAttributes readAttributes() throws IOException {
		return new BasicFileAttributesImpl(path);
	}

	@Override
	public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime,
			FileTime createTime) throws IOException {
		if (lastModifiedTime == null || lastAccessTime == null) {
			BasicFileAttributes attributes = readAttributes();
			if (lastModifiedTime == null)
				lastModifiedTime = attributes.lastModifiedTime();
			if (lastAccessTime == null)
				lastAccessTime = attributes.lastAccessTime();
		}
		((HadoopFileSystem) path.getFileSystem()).getFileContext().setTimes(
				((HadoopFileSystemPath) path).getPath(),
				lastModifiedTime.toMillis(), lastAccessTime.toMillis());

	}

	Map<String, Object> readAttributes(String attributes) throws IOException {
		BasicFileAttributes attr = readAttributes();
		List<String> attrlist = Arrays.asList(attributes.split(","));
		boolean readall = attrlist.contains("*");
		Map<String, Object> ret = new HashMap<>();
		if (readall || attrlist.contains("fileKey"))
			ret.put("fileKey", attr.fileKey());
		if (readall || attrlist.contains("creationTime"))
			ret.put("creationTime", attr.creationTime());
		if (readall || attrlist.contains("isDirectory"))
			ret.put("isDirectory", attr.isDirectory());
		if (readall || attrlist.contains("isOther"))
			ret.put("isOther", attr.isOther());
		if (readall || attrlist.contains("isRegularFile"))
			ret.put("isRegularFile", attr.isRegularFile());
		if (readall || attrlist.contains("isSymbolicLink"))
			ret.put("isSymbolicLink", attr.isSymbolicLink());
		if (readall || attrlist.contains("lastAccessTime"))
			ret.put("lastAccessTime", attr.lastAccessTime());
		if (readall || attrlist.contains("lastModifiedTime"))
			ret.put("lastModifiedTime", attr.lastModifiedTime());
		if (readall || attrlist.contains("size"))
			ret.put("size", attr.size());
		return ret;
	}

}
