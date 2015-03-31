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
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.spi.FileSystemProvider;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class HadoopFileAttributeViewImpl extends PosixFileAttributeViewImpl implements
		HadoopFileAttributeView {
	static final String NAME = HadoopFileSystem.SCHEME;

	public HadoopFileAttributeViewImpl(Path path) {
		super(path);
	}

	@Override
	public HadoopFileAttributes readAttributes() throws IOException {
		return new HadoopFileAttributesImpl(path);
	}

	@Override
	public String name() {
		return NAME;
	}

	/**
	 * @see FileSystemProvider#readAttributes(java.nio.file.Path, String,
	 *      java.nio.file.LinkOption...)
	 * @param attributes
	 * @return
	 * @throws IOException
	 */
	@Override
	Map<String, Object> readAttributes(String attributes) throws IOException {
		HadoopFileAttributes attr = readAttributes();
		List<String> attrlist = Arrays.asList(attributes.split(","));
		boolean readall = attrlist.contains("*");
		Map<String, Object> ret = new HashMap<>();
		if (readall || attrlist.contains("isHidden"))
			ret.put("isHidden", attr.isHidden());
		if (readall || attrlist.contains("blockSize"))
			ret.put("blockSize", attr.getBlockSize());
		if (readall || attrlist.contains("replication"))
			ret.put("replication", attr.getReplication());
		return ret;

	}

	@SuppressWarnings("unchecked")
	void setAttribute(String attribute, Object value) throws IOException {
		switch (attribute) {
		case NAME + ":replication":
			setReplication((Short) value);
			break;
		case PosixFileAttributeViewImpl.NAME + ":permissions":
			setPermissions((Set<PosixFilePermission>) value);
			break;
		case "owner":
		case BasicFileAttributeViewImpl.NAME + ":owner":
			setOwner((UserPrincipal) value);
			break;
		case "group":
		case BasicFileAttributeViewImpl.NAME + ":group":
			setOwner((UserPrincipal) value);
			break;
		case "lastModifiedTime":
		case BasicFileAttributeViewImpl.NAME + ":lastModifiedTime":
			setTimes((FileTime) value, null, null);
			break;

		case "lastAccessTime":
		case BasicFileAttributeViewImpl.NAME + ":lastAccessTime":
			setTimes(null, (FileTime) value, null);
			break;

		case "createTime":
		case BasicFileAttributeViewImpl.NAME + ":createTime":
			setTimes(null, null, (FileTime) value);
			break;

		default:
			throw new IllegalArgumentException(String.format(
					"attribute %s not supported", attribute));
		}

	}

	@Override
	public void setReplication(short replication) throws IOException {
		((HadoopFileSystem) path.getFileSystem()).getFileContext()
				.setReplication(((HadoopFileSystemPath) path).getPath(),
						replication);

	}

}
