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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.AccessMode;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.StandardOpenOption;
import java.nio.file.WatchService;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.GlobPattern;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

public final class HadoopFileSystem extends FileSystem {

	private class WrappedInputChannel implements SeekableByteChannel {
		private final FSDataInputStream source;
		private final ReadableByteChannel channel;
		private final org.apache.hadoop.fs.Path path;
		private final boolean deleteOnClose;
		private final boolean byteBufferReadable;
		private final int bufferSize;

		public WrappedInputChannel(HadoopFileSystemPath path,
				Set<? extends OpenOption> options, FileAttribute<?>... attrs)
				throws IOException {
			this.path = path.getPath();

			if (options.contains(StandardOpenOption.WRITE))
				throw new UnsupportedOperationException();

			deleteOnClose = options
					.contains(StandardOpenOption.DELETE_ON_CLOSE);
			if (deleteOnClose) {
				fileContext.deleteOnExit(this.path);
			}

			source = fileContext.open(path.getPath());
			byteBufferReadable = ByteBufferReadable.class.isInstance(source
					.getWrappedStream());

			if (!byteBufferReadable) {
				bufferSize = ((HadoopFileSystemProvider) HadoopFileSystem.this
						.provider()).getConfiguration().getInt(
						DFSConfigKeys.DFS_STREAM_BUFFER_SIZE_KEY,
						DFSConfigKeys.DFS_STREAM_BUFFER_SIZE_DEFAULT);

			} else {
				bufferSize = 0;
			}
			channel = Channels.newChannel(source);
		}

		@Override
		public boolean isOpen() {
			return channel.isOpen();
		}

		@Override
		public void close() throws IOException {
			channel.close();
			if (deleteOnClose)
				fileContext.delete(path, false);

		}

		@Override
		public int read(ByteBuffer dst) throws IOException {
			if (byteBufferReadable)
				return source.read(dst);
			if (dst.hasArray()) {
				int pos = dst.position() + dst.arrayOffset();
				int read = source.getWrappedStream().read(dst.array(), pos,
						dst.remaining());
				if (read >= 0)
					dst.position(pos + read);
				return read;
			}
			// ok we have to use an other array
			byte[] buffer = new byte[bufferSize];
			int read;
			int overall = 0;
			while ((read = source.getWrappedStream().read(buffer, 0,
					Math.min(bufferSize, dst.remaining()))) > 0) {
				dst.put(buffer, 0, read);
				overall += read;
			}
			;
			return overall;

		}

		@Override
		public int write(ByteBuffer src) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public long position() throws IOException {
			return source.getPos();
		}

		@Override
		public SeekableByteChannel position(long newPosition)
				throws IOException {
			source.seekToNewSource(newPosition);
			return this;
		}

		@Override
		public long size() throws IOException {
			return fileContext.getFileStatus(path).getLen();
		}

		@Override
		public SeekableByteChannel truncate(long size) throws IOException {
			throw new UnsupportedOperationException();
		}

	}

	private class WrappedOutputChannel implements SeekableByteChannel {
		private final FSDataOutputStream source;
		private final WritableByteChannel channel;
		private final org.apache.hadoop.fs.Path path;
		private final boolean deleteOnClose;

		public WrappedOutputChannel(HadoopFileSystemPath path,
				Set<? extends OpenOption> options, FileAttribute<?>... attrs)
				throws IOException {
			this.path = path.getPath();
			if (options.contains(StandardOpenOption.READ))
				throw new UnsupportedOperationException();
			EnumSet<CreateFlag> flags = EnumSet.of(CreateFlag.CREATE);
			if (options.contains(StandardOpenOption.APPEND))
				flags.add(CreateFlag.APPEND);
			else if (!options.contains(StandardOpenOption.CREATE_NEW)
					|| options.contains(StandardOpenOption.TRUNCATE_EXISTING))
				flags.add(CreateFlag.OVERWRITE);
			if (options.contains(StandardOpenOption.DSYNC)
					|| options.contains(StandardOpenOption.SYNC))
				flags.add(CreateFlag.SYNC_BLOCK);
			deleteOnClose = options
					.contains(StandardOpenOption.DELETE_ON_CLOSE);
			if (deleteOnClose) {
				fileContext.deleteOnExit(this.path);
			}
			List<CreateOpts> createOpts = new ArrayList<>();
			createOpts.add(CreateOpts.perms(fromFileAttributes(attrs)));

			long blocksize = ((HadoopFileSystemProvider) provider())
					.getConfiguration().getLong(
							DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
							DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);

			short replication = (short) ((HadoopFileSystemProvider) provider())
					.getConfiguration().getInt(
							DFSConfigKeys.DFS_REPLICATION_KEY,
							DFSConfigKeys.DFS_REPLICATION_DEFAULT);

			for (FileAttribute<?> attr : attrs) {
				if ((HadoopFileAttributeViewImpl.NAME + ":blockSize")
						.equals(attr.name())
						&& Long.class.isInstance(attr.value())) {
					blocksize = (Long) attr.value();
				}
				if ((HadoopFileAttributeViewImpl.NAME + ":replication")
						.equals(attr.name())
						&& Short.class.isInstance(attr.value())) {
					replication = (Short) attr.value();
				}
			}

			int maxRepl = (int) ((HadoopFileSystemProvider) provider())
					.getConfiguration().getInt(
							DFSConfigKeys.DFS_REPLICATION_MAX_KEY,
							DFSConfigKeys.DFS_REPLICATION_MAX_DEFAULT);

			createOpts.add(CreateOpts.blockSize(blocksize));
			createOpts.add(CreateOpts.repFac((short) Math.min(replication,
					maxRepl)));
			source = fileContext.create(path.getPath(), flags,
					createOpts.toArray(new CreateOpts[createOpts.size()]));
			channel = Channels.newChannel(source);
		}

		@Override
		public boolean isOpen() {
			return channel.isOpen();
		}

		@Override
		public void close() throws IOException {
			channel.close();
			if (deleteOnClose)
				fileContext.delete(path, false);
		}

		@Override
		public int read(ByteBuffer dst) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public int write(ByteBuffer src) throws IOException {
			return channel.write(src);
		}

		@Override
		public long position() throws IOException {
			return source.getPos();
		}

		@Override
		public SeekableByteChannel position(long newPosition)
				throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public long size() throws IOException {
			return source.size();
		}

		@Override
		public SeekableByteChannel truncate(long size) throws IOException {
			throw new UnsupportedOperationException();
		}

	}

	private final HadoopFileSystemProvider provider;
	private final FileContext fileContext;
	private boolean isClosed = false;
	static final String SCHEME = HdfsConstants.HDFS_URI_SCHEME;

	HadoopFileSystem(HadoopFileSystemProvider provider, URI uri)
			throws UnsupportedFileSystemException {
		this(provider, uri, new Configuration());

	}

	HadoopFileSystem(HadoopFileSystemProvider provider, URI uri,
			Configuration configuration) throws UnsupportedFileSystemException {
		if (provider == null)
			throw new NullPointerException();
		this.provider = provider;
		fileContext = FileContext.getFileContext(uri, configuration);

	}

	FileContext getFileContext() {
		return fileContext;
	}

	@Override
	public FileSystemProvider provider() {
		return provider;
	}

	@Override
	public void close() throws IOException {
		provider.unregister(this);
		isClosed = true;

	}

	@Override
	public boolean isOpen() {
		return !isClosed;
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

	@Override
	public String getSeparator() {
		return org.apache.hadoop.fs.Path.SEPARATOR;
	}

	@Override
	public Iterable<Path> getRootDirectories() {
		// TODO Securitymanager
		return Arrays.asList(getPath("/"));
	}

	@Override
	public Iterable<FileStore> getFileStores() {
		List<FileStore> fileStores = new ArrayList<>();
		fileStores.add(new HadoopFileStore(this));
		return fileStores;
	}

	@Override
	public Set<String> supportedFileAttributeViews() {
		Set<String> s = new HashSet<>();
		s.add(BasicFileAttributeViewImpl.NAME);
		s.add(PosixFileAttributeViewImpl.NAME);
		s.add(HadoopFileAttributeViewImpl.NAME);
		return s;
	}

	@Override
	public Path getPath(String first, String... more) {
		Path path = new HadoopFileSystemPath(this, URI.create(Objects.toString(
				first, getSeparator())));
		for (String p : more) {
			path = path.resolve(p);
		}
		return path;

	}

	@Override
	public PathMatcher getPathMatcher(String syntaxAndPattern) {
		String syntax = "glob";
		String pattern = syntaxAndPattern;
		String[] sp = syntaxAndPattern.split(":", 2);
		if (sp.length == 2) {
			syntax = sp[0];
			pattern = sp[1];
		}
		if (!"glob".equals(syntax))
			throw new UnsupportedOperationException();

		final GlobPattern globPattern = new GlobPattern(pattern);
		return new PathMatcher() {
			@Override
			public boolean matches(Path path) {
				return globPattern.matches(path.toString());
			}
		};
	}

	@Override
	public UserPrincipalLookupService getUserPrincipalLookupService() {
		throw new UnsupportedOperationException();
		/*
		 * fileContext.getDefaultFileSystem().getDelegationTokens(null).get(0).
		 * isManaged() return new UserPrincipalLookupService() {
		 * 
		 * @Override public UserPrincipal lookupPrincipalByName(String name)
		 * throws IOException { return new HadoopUserPrincipal(name); }
		 * 
		 * @Override public GroupPrincipal lookupPrincipalByGroupName(String
		 * group) throws IOException { return new HadoopGroupPrincipal(group); }
		 * };
		 */
	}

	@Override
	public WatchService newWatchService() throws IOException {
		throw new UnsupportedOperationException();
	}

	SeekableByteChannel newByteChannel(Path path,
			Set<? extends OpenOption> options, FileAttribute<?>... attrs)
			throws IOException {
		if (options.contains(StandardOpenOption.READ)
				|| !options.contains(StandardOpenOption.WRITE))
			return new WrappedInputChannel((HadoopFileSystemPath) path,
					options, attrs);
		return new WrappedOutputChannel((HadoopFileSystemPath) path, options,
				attrs);
	}

	void delete(Path path, boolean recursive) throws IOException {
		fileContext.delete(((HadoopFileSystemPath) path).getPath(), recursive);
	}

	static FsPermission fromPosixPermissions(
			Set<PosixFilePermission> permissions) {
		FsAction u = FsAction.NONE, g = FsAction.NONE, o = FsAction.NONE;
		for (PosixFilePermission permission : permissions) {
			switch (permission) {
			case GROUP_EXECUTE:
				g = g.or(FsAction.EXECUTE);
				break;
			case GROUP_READ:
				g = g.or(FsAction.READ);
				break;
			case GROUP_WRITE:
				g = g.or(FsAction.WRITE);
				break;
			case OWNER_EXECUTE:
				u = u.or(FsAction.EXECUTE);
				break;
			case OWNER_READ:
				u = u.or(FsAction.READ);
				break;
			case OWNER_WRITE:
				u = u.or(FsAction.WRITE);
				break;
			case OTHERS_EXECUTE:
				o = o.or(FsAction.EXECUTE);
				break;
			case OTHERS_READ:
				o = o.or(FsAction.READ);
				break;
			case OTHERS_WRITE:
				o = o.or(FsAction.WRITE);
				break;
			default:
				break;
			}

		}
		return new FsPermission(u, g, o);

	}

	static FsPermission fromFileAttributes(FileAttribute<?>... attrs) {
		for (FileAttribute<?> attribute : attrs) {
			if ("posix:permissions".equals(attribute.name())) {
				@SuppressWarnings("unchecked")
				Set<PosixFilePermission> permissions = (Set<PosixFilePermission>) attribute
						.value();
				return fromPosixPermissions(permissions);
			}
		}
		return FsPermission.getDefault();

	}

	void createDirectory(Path dir, FileAttribute<?>... attrs)
			throws IOException {
		try {
			fileContext.mkdir(((HadoopFileSystemPath) dir).getPath(),
					fromFileAttributes(attrs), false);
		} catch (FileAlreadyExistsException e) {
			if (Files.isDirectory(dir)) {
				return;
			}
			throw new java.nio.file.FileAlreadyExistsException(dir.toString());
		}
	}

	DirectoryStream<Path> newDirectoryStream(final Path dir,
			final Filter<? super Path> filter) throws IOException {

		final RemoteIterator<FileStatus> iter = getFileContext().listStatus(
				((HadoopFileSystemPath) dir).getPath());

		final Predicate<Path> predicate = new Predicate<Path>() {
			@Override
			public boolean apply(@Nullable Path input) {
				try {
					return filter.accept(input);
				} catch (IOException e) {
					throw new RuntimeException(e.getLocalizedMessage(), e);
				}
			}
		};

		final Iterator<Path> iterator = new Iterator<Path>() {

			@Override
			public boolean hasNext() {
				try {
					return iter.hasNext();
				} catch (IOException e) {
					return false;
				}
			}

			@Override
			public Path next() {
				try {
					return new HadoopFileSystemPath(
							(HadoopFileSystem) dir.getFileSystem(), iter.next()
									.getPath().toUri());
				} catch (IOException e) {
					throw new RuntimeException(e.getLocalizedMessage(), e);
				}
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException("remove");

			}
		};

		return new DirectoryStream<Path>() {

			@Override
			public void close() throws IOException {
			}

			@Override
			public Iterator<Path> iterator() {
				return Iterators.filter(iterator, predicate);
			}
		};
	}

	boolean isSameFile(Path path, Path path2) throws IOException {
		if (!path.getFileSystem().equals(path2.getFileSystem()))
			return false;
		return path.toRealPath().equals(path2.toRealPath());

	}

	void checkAccess(Path path, AccessMode... modes) throws IOException {
		try {
			FileStatus fileStatus = getFileContext().getFileStatus(
					((HadoopFileSystemPath) path).getPath());
			if (modes == null || modes.length == 0)
				return;

			String group = fileStatus.getGroup();
			String owner = fileStatus.getOwner();
			UserGroupInformation userGroupInformation = getFileContext()
					.getUgi();

			boolean checkuser = false;
			boolean checkgroup = false;

			if (owner.equals(userGroupInformation.getUserName())) {
				checkuser = true;
			} else {
				for (String g : userGroupInformation.getGroupNames()) {
					if (group.equals(g)) {
						checkgroup = true;
						break;
					}

				}

			}

			PosixFileAttributeView view = provider().getFileAttributeView(path,
					PosixFileAttributeView.class);
			PosixFileAttributes attributes = view.readAttributes();
			Set<PosixFilePermission> permissions = attributes.permissions();

			getFileContext().getUgi().getGroupNames();
			for (AccessMode accessMode : modes) {
				switch (accessMode) {
				case READ:
					if (!permissions
							.contains(checkuser ? PosixFilePermission.OWNER_READ
									: (checkgroup ? PosixFilePermission.GROUP_READ
											: PosixFilePermission.OTHERS_READ)))
						throw new AccessDeniedException(path.toString());
					break;
				case WRITE:
					if (!permissions
							.contains(checkuser ? PosixFilePermission.OWNER_WRITE
									: (checkgroup ? PosixFilePermission.GROUP_WRITE
											: PosixFilePermission.OTHERS_WRITE)))
						throw new AccessDeniedException(path.toString());
					break;
				case EXECUTE:
					if (!permissions
							.contains(checkuser ? PosixFilePermission.OWNER_EXECUTE
									: (checkgroup ? PosixFilePermission.GROUP_EXECUTE
											: PosixFilePermission.OTHERS_EXECUTE)))
						throw new AccessDeniedException(path.toString());
					break;
				}
			}

		} catch (FileNotFoundException e) {
			throw new NoSuchFileException(path.toString());
		}
	}
}
