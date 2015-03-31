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
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

class PosixAttributesImpl extends BasicFileAttributesImpl implements
		PosixFileAttributes {

	public PosixAttributesImpl(HadoopFileSystemPath path) throws IOException {
		super(path);
	}

	@Override
	public UserPrincipal owner() {
		return new HadoopUserPrincipal(fileStatus.getOwner());
	}

	@Override
	public GroupPrincipal group() {
		return new HadoopGroupPrincipal(fileStatus.getGroup());
	}

	@Override
	public Set<PosixFilePermission> permissions() {
		Set<PosixFilePermission> permissions = new HashSet<>();
		FsPermission permission = fileStatus.getPermission();
		FsAction action = permission.getUserAction();
		if (action != null) {
			int bits = action.ordinal();
			if ((bits & FsAction.EXECUTE.ordinal()) > 0)
				permissions.add(PosixFilePermission.OWNER_EXECUTE);
			if ((bits & FsAction.WRITE.ordinal()) > 0)
				permissions.add(PosixFilePermission.OWNER_WRITE);
			if ((bits & FsAction.READ.ordinal()) > 0)
				permissions.add(PosixFilePermission.OWNER_READ);
		}

		action = permission.getGroupAction();
		if (action != null) {
			int bits = action.ordinal();
			if ((bits & FsAction.EXECUTE.ordinal()) > 0)
				permissions.add(PosixFilePermission.GROUP_EXECUTE);
			if ((bits & FsAction.WRITE.ordinal()) > 0)
				permissions.add(PosixFilePermission.GROUP_WRITE);
			if ((bits & FsAction.READ.ordinal()) > 0)
				permissions.add(PosixFilePermission.GROUP_READ);
		}

		action = permission.getOtherAction();
		if (action != null) {
			int bits = action.ordinal();
			if ((bits & FsAction.EXECUTE.ordinal()) > 0)
				permissions.add(PosixFilePermission.OTHERS_EXECUTE);
			if ((bits & FsAction.WRITE.ordinal()) > 0)
				permissions.add(PosixFilePermission.OTHERS_WRITE);
			if ((bits & FsAction.READ.ordinal()) > 0)
				permissions.add(PosixFilePermission.OTHERS_READ);
		}

		return permissions;
	}

}
