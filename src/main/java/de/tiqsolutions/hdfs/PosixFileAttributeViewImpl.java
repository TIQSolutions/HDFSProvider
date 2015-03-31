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
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class PosixFileAttributeViewImpl extends BasicFileAttributeViewImpl implements
		PosixFileAttributeView {

	static final String NAME = "posix";

	public PosixFileAttributeViewImpl(Path path) {
		super(path);
	}

	@Override
	public PosixFileAttributes readAttributes() throws IOException {
		return new PosixAttributesImpl(path);
	}

	@Override
	public UserPrincipal getOwner() throws IOException {
		return readAttributes().owner();
	}

	@Override
	public void setOwner(UserPrincipal owner) throws IOException {
		((HadoopFileSystem) path.getFileSystem()).getFileContext().setOwner(
				((HadoopFileSystemPath) path).getPath(), owner.getName(), null);

	}

	@Override
	public void setPermissions(Set<PosixFilePermission> perms)
			throws IOException {
		((HadoopFileSystem) path.getFileSystem()).getFileContext()
				.setPermission(((HadoopFileSystemPath) path).getPath(),
						HadoopFileSystem.fromPosixPermissions(perms));

	}

	@Override
	public void setGroup(GroupPrincipal group) throws IOException {
		((HadoopFileSystem) path.getFileSystem()).getFileContext().setOwner(
				((HadoopFileSystemPath) path).getPath(), group.getName(), null);

	}

	@Override
	public String name() {
		return NAME;
	}

	Map<String, Object> readAttributes(String attributes) throws IOException {
		PosixFileAttributes attr = readAttributes();
		List<String> attrlist = Arrays.asList(attributes.split(","));
		boolean readall = attrlist.contains("*");
		Map<String, Object> ret = new HashMap<>();
		if (readall || attrlist.contains("owner"))
			ret.put("owner", attr.owner());
		if (readall || attrlist.contains("group"))
			ret.put("group", attr.group());
		if (readall || attrlist.contains("permissions"))
			ret.put("permissions", attr.permissions());
		return ret;
	}

}
