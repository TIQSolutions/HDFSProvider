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
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.NotLinkException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.spi.FileSystemProvider;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.xml.bind.annotation.adapters.HexBinaryAdapter;

import org.apache.commons.lang.NullArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;

public class HadoopFileSystemProvider extends FileSystemProvider {

	private final Map<String, HadoopFileSystem> fileSystems = Collections
			.synchronizedMap(new HashMap<String, HadoopFileSystem>(5));

	@Override
	public String getScheme() {
		return HadoopFileSystem.SCHEME;
	}

	private void checkURI(URI uri) {
		if (uri == null)
			throw new NullPointerException();
		if (!getScheme().equalsIgnoreCase(uri.getScheme())
				&& !"webhdfs".equalsIgnoreCase(uri.getScheme()))
			throw new IllegalArgumentException(String.format(
					"Scheme %s not supported", uri.getScheme()));
	}

	private String getURIKey(URI uri) {
		String s = String.format("%s://%s@%s:%d", getScheme(),
				uri.getUserInfo() == null ? "" : uri.getUserInfo(),
				uri.getHost(), uri.getPort());
		try {
			MessageDigest cript = MessageDigest.getInstance("SHA-1");
			cript.reset();
			cript.update(s.getBytes("utf8"));
			return new HexBinaryAdapter().marshal(cript.digest());
		} catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
		}
		return null;

	}

	void unregister(HadoopFileSystem fileSystem) {
		for (Map.Entry<String, HadoopFileSystem> fs : fileSystems.entrySet()) {
			if (fileSystem.equals(fs.getValue())) {
				fileSystems.remove(fs.getKey());
			}
		}

	}

	protected Configuration getConfiguration() {
		return new Configuration();
	}

	@Override
	public FileSystem newFileSystem(URI uri, Map<String, ?> env)
			throws IOException {
		checkURI(uri);
		String key = getURIKey(uri);
		if (fileSystems.containsKey(key))
			throw new FileSystemAlreadyExistsException();
		Configuration configuration = getConfiguration();

		HadoopFileSystem fs = null;
		if (configuration == null)
			configuration = new Configuration();

		for (Map.Entry<String, ?> entry : env.entrySet())
			configuration.set(entry.getKey(), entry.getValue().toString());
		fs = new HadoopFileSystem(this, uri, configuration);

		fileSystems.put(key, fs);
		return fs;
	}

	@Override
	public FileSystem getFileSystem(URI uri) {
		checkURI(uri);
		String key = getURIKey(uri);
		FileSystem fs = fileSystems.get(key);
		if (fs == null)
			throw new FileSystemNotFoundException();
		return fs;
	}

	@Override
	public Path getPath(URI uri) {
		checkURI(uri);
		return getFileSystem(uri).getPath(uri.getPath());
	}

	@Override
	public SeekableByteChannel newByteChannel(Path path,
			Set<? extends OpenOption> options, FileAttribute<?>... attrs)
			throws IOException {
		FileSystem fs = path.getFileSystem();
		if (!HadoopFileSystem.class.isInstance(fs))
			throw new IllegalArgumentException("path");
		try {
			return ((HadoopFileSystem) fs).newByteChannel(path, options, attrs);
		} catch (RemoteException e) {
			rethrowRemoteException(e, path);
			return null;
		}
	}

	@Override
	public DirectoryStream<Path> newDirectoryStream(Path dir,
			Filter<? super Path> filter) throws IOException {
		FileSystem fs = dir.getFileSystem();
		if (!HadoopFileSystem.class.isInstance(fs))
			throw new IllegalArgumentException("dir");
		try {
			return ((HadoopFileSystem) fs).newDirectoryStream(dir, filter);
		} catch (RemoteException e) {
			rethrowRemoteException(e, dir);
			return null;
		}
	}

	@Override
	public void createDirectory(Path dir, FileAttribute<?>... attrs)
			throws IOException {
		FileSystem fs = dir.getFileSystem();
		if (!HadoopFileSystem.class.isInstance(fs))
			throw new IllegalArgumentException("dir");
		try {
			((HadoopFileSystem) fs).createDirectory(dir, attrs);
		} catch (RemoteException e) {
			rethrowRemoteException(e, dir);
		}
	}

	static void rethrowRemoteException(RemoteException e, Path p1, Path p2)
			throws IOException {
		switch (e.getClassName()) {
		case "org.apache.hadoop.fs.PathIsNotEmptyDirectoryException":
			throw new DirectoryNotEmptyException(p1.toString());

		case "org.apache.hadoop.fs.PathExistsException":
		case "org.apache.hadoop.fs.FileAlreadyExistsException":
			throw new FileAlreadyExistsException(Objects.toString(p1),
					Objects.toString(p2), e.getLocalizedMessage());

		case "org.apache.hadoop.fs.PathPermissionException":
		case "org.apache.hadoop.fs.PathAccessDeniedException":
			throw new AccessDeniedException(Objects.toString(p1),
					Objects.toString(p2), e.getLocalizedMessage());

		case "org.apache.hadoop.fs.ParentNotDirectoryException":
		case "org.apache.hadoop.fs.DirectoryListingStartAfterNotFoundException":
		case "org.apache.hadoop.fs.PathIsNotDirectoryException":
			throw new NotDirectoryException(Objects.toString(p1));

		case "org.apache.hadoop.fs.PathIsDirectoryException":
		case "org.apache.hadoop.fs.InvalidPathException":
		case "org.apache.hadoop.fs.PathNotFoundException":
			throw new NoSuchFileException(Objects.toString(p1),
					Objects.toString(p2), e.getLocalizedMessage());

		case "org.apache.hadoop.fs.UnresolvedLinkException":
			throw new NotLinkException(Objects.toString(p1),
					Objects.toString(p2), e.getLocalizedMessage());

		case "org.apache.hadoop.fs.PathIOException":
		case "org.apache.hadoop.fs.ChecksumException":
		case "org.apache.hadoop.fs.InvalidRequestException":
		case "org.apache.hadoop.fs.UnsupportedFileSystemException":
		case "org.apache.hadoop.fs.ZeroCopyUnavailableException":

		}

		throw new IOException(e.getLocalizedMessage(), e);
	}

	static void rethrowRemoteException(RemoteException e, Path p)
			throws IOException {
		rethrowRemoteException(e, p, null);
	}

	public void delete(Path path, boolean recursive) throws IOException {
		FileSystem fs = path.getFileSystem();
		if (!HadoopFileSystem.class.isInstance(fs))
			throw new IllegalArgumentException("path");
		try {
			((HadoopFileSystem) fs).delete(path, recursive);
		} catch (RemoteException e) {
			rethrowRemoteException(e, path);
		}
	}

	@Override
	public void delete(Path path) throws IOException {
		delete(path, false);
	}

	private void remoteCopy(Path source, Path target, CopyOption... options)
			throws IOException {
		Configuration configuration = getConfiguration();
		Path tmp = target.getParent();
		Path dest = null;
		do {
			dest = tmp.resolve(String.format("tmp%s/",
					System.currentTimeMillis()));
		} while (Files.exists(dest));
		try {
			DistCpOptions distCpOptions = new DistCpOptions(
					Arrays.asList(((HadoopFileSystemPath) source).getPath()),
					((HadoopFileSystemPath) dest).getPath());
			List<CopyOption> optionList = Arrays.asList(options);

			distCpOptions.setOverwrite(optionList
					.contains(StandardCopyOption.REPLACE_EXISTING));
			try {
				DistCp distCp = new DistCp(configuration, distCpOptions);
				Job job = distCp.execute();
				job.waitForCompletion(true);
			} catch (Exception e) {
				throw new IOException(e.getLocalizedMessage(), e);
			}
			move(dest.resolve(source.getFileName()), target, options);
		} finally {
			delete(dest, false);
		}

	}

	@Override
	public void copy(Path source, Path target, CopyOption... options)
			throws IOException {
		List<CopyOption> optionList = Arrays.asList(options);
		if (!optionList.contains(StandardCopyOption.REPLACE_EXISTING)) {
			if (Files.exists(target))
				throw new java.nio.file.FileAlreadyExistsException(
						source.toString(), target.toString(),
						"could not copy file to destination");
		} else {
			Files.deleteIfExists(target);
		}

		FileSystem sourceFS = source.getFileSystem();
		FileSystem targetFS = target.getFileSystem();

		if (optionList.contains(HadoopCopyOption.REMOTE_COPY)
				&& sourceFS.equals(targetFS)) {

			remoteCopy(source, target, options);
			return;

		}
		try (SeekableByteChannel sourceChannel = sourceFS.provider()
				.newByteChannel(source, EnumSet.of(StandardOpenOption.READ))) {

			Set<StandardOpenOption> openOptions = EnumSet
					.of(StandardOpenOption.WRITE);

			if (optionList.contains(StandardCopyOption.REPLACE_EXISTING))
				openOptions.add(StandardOpenOption.CREATE);
			else
				openOptions.add(StandardOpenOption.CREATE_NEW);
			List<FileAttribute<?>> fileAttributes = new ArrayList<>();
			if (optionList.contains(StandardCopyOption.COPY_ATTRIBUTES)) {

				Set<String> sourceAttrViews = sourceFS
						.supportedFileAttributeViews();
				Set<String> targetAttrViews = targetFS
						.supportedFileAttributeViews();
				if (sourceAttrViews.contains(PosixFileAttributeViewImpl.NAME)
						&& targetAttrViews
								.contains(PosixFileAttributeViewImpl.NAME)) {
					PosixFileAttributes posixAttributes = sourceFS.provider()
							.readAttributes(source, PosixFileAttributes.class);
					fileAttributes.add(PosixFilePermissions
							.asFileAttribute(posixAttributes.permissions()));
				}

				if (sourceAttrViews.contains(HadoopFileAttributeViewImpl.NAME)
						&& targetAttrViews
								.contains(HadoopFileAttributeViewImpl.NAME)) {
					final HadoopFileAttributes hdfsAttributes = sourceFS
							.provider().readAttributes(source,
									HadoopFileAttributes.class);
					fileAttributes.add(new FileAttribute<Long>() {
						@Override
						public String name() {
							return HadoopFileAttributeViewImpl.NAME
									+ ":blockSize";
						}

						@Override
						public Long value() {
							return hdfsAttributes.getBlockSize();
						}
					});
					fileAttributes.add(new FileAttribute<Short>() {
						@Override
						public String name() {
							return HadoopFileAttributeViewImpl.NAME
									+ ":replication";
						}

						@Override
						public Short value() {
							return hdfsAttributes.getReplication();
						}
					});

				}
			}

			FileAttribute<?>[] attributes = fileAttributes
					.toArray(new FileAttribute<?>[fileAttributes.size()]);

			try (SeekableByteChannel targetChannel = targetFS.provider()
					.newByteChannel(target, openOptions, attributes)) {
				int buffSize = getConfiguration().getInt(
						DFSConfigKeys.DFS_STREAM_BUFFER_SIZE_KEY,
						DFSConfigKeys.DFS_STREAM_BUFFER_SIZE_DEFAULT);
				ByteBuffer buffer = ByteBuffer.allocate(buffSize);
				buffer.clear();
				while (sourceChannel.read(buffer) > 0) {
					buffer.flip();
					targetChannel.write(buffer);
					buffer.clear();
				}

			}
			if (optionList.contains(StandardCopyOption.COPY_ATTRIBUTES)) {
				BasicFileAttributes attrs = sourceFS.provider().readAttributes(
						source, BasicFileAttributes.class);
				BasicFileAttributeView view = targetFS.provider()
						.getFileAttributeView(target,
								BasicFileAttributeView.class);
				view.setTimes(attrs.lastModifiedTime(), attrs.lastAccessTime(),
						attrs.creationTime());

			}

		}

	}

	@Override
	public void move(Path source, Path target, CopyOption... options)
			throws IOException {
		FileSystem fs = source.getFileSystem();
		if (!HadoopFileSystem.class.isInstance(fs))
			throw new IllegalArgumentException("source");
		if (!fs.provider().equals(target.getFileSystem().provider()))
			throw new ProviderMismatchException();
		List<Rename> renameOptions = new ArrayList<>();
		List<CopyOption> copyOptions = Arrays.asList(options);

		if (copyOptions.contains(StandardCopyOption.REPLACE_EXISTING))
			renameOptions.add(Rename.OVERWRITE);
		try {
			((HadoopFileSystem) fs).getFileContext().rename(
					((HadoopFileSystemPath) source).getPath(),
					((HadoopFileSystemPath) target).getPath(),
					renameOptions.toArray(new Rename[renameOptions.size()]));
		} catch (RemoteException e) {
			rethrowRemoteException(e, source, target);

		}
	}

	@Override
	public boolean isSameFile(Path path, Path path2) throws IOException {
		if (path == null)
			throw new NullArgumentException("path");
		if (path2 == null)
			throw new NullArgumentException("path2");
		FileSystem fs = path.getFileSystem();
		if (!HadoopFileSystem.class.isInstance(fs))
			throw new IllegalArgumentException("path");
		return ((HadoopFileSystem) fs).isSameFile(path, path2);

	}

	@Override
	public boolean isHidden(Path path) throws IOException {
		FileSystem fs = path.getFileSystem();
		if (!HadoopFileSystem.class.isInstance(fs))
			throw new IllegalArgumentException("path");
		try {
			return readAttributes(path, HadoopFileAttributes.class).isHidden();
		} catch (RemoteException e) {
			rethrowRemoteException(e, path);
			return false;
		}
	}

	@Override
	public FileStore getFileStore(Path path) throws IOException {
		for (FileStore fs : path.getFileSystem().getFileStores()) {
			return fs;
		}
		return null;
	}

	@Override
	public void checkAccess(Path path, AccessMode... modes) throws IOException {
		FileSystem fs = path.getFileSystem();
		if (!HadoopFileSystem.class.isInstance(fs))
			throw new IllegalArgumentException("path");
		try {
			((HadoopFileSystem) fs).checkAccess(path, modes);
		} catch (RemoteException e) {
			rethrowRemoteException(e, path);

		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V extends FileAttributeView> V getFileAttributeView(Path path,
			Class<V> type, LinkOption... options) {
		if (BasicFileAttributeView.class.equals(type))
			return (V) new BasicFileAttributeViewImpl(path);
		if (PosixFileAttributeView.class.equals(type))
			return (V) new PosixFileAttributeViewImpl(path);
		if (HadoopFileAttributeView.class.equals(type))
			return (V) new HadoopFileAttributeViewImpl(path);
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <A extends BasicFileAttributes> A readAttributes(Path path,
			Class<A> type, LinkOption... options) throws IOException {
		if (BasicFileAttributes.class.equals(type))
			return (A) getFileAttributeView(path, BasicFileAttributeView.class,
					options).readAttributes();
		if (PosixFileAttributes.class.equals(type))
			return (A) getFileAttributeView(path, PosixFileAttributeView.class,
					options).readAttributes();
		if (HadoopFileAttributes.class.equals(type))
			return (A) getFileAttributeView(path,
					HadoopFileAttributeView.class, options).readAttributes();
		return null;
	}

	@Override
	public Map<String, Object> readAttributes(Path path, String attributes,
			LinkOption... options) throws IOException {
		if (attributes == null)
			throw new NullArgumentException("attributes");
		String[] args = attributes.split(":", 2);

		if (args.length == 2) {
			switch (args[0]) {
			case BasicFileAttributeViewImpl.NAME:
				return ((BasicFileAttributeViewImpl) getFileAttributeView(path,
						BasicFileAttributeView.class, options))
						.readAttributes(args[1]);
			case PosixFileAttributeViewImpl.NAME:
				return ((PosixFileAttributeViewImpl) getFileAttributeView(path,
						PosixFileAttributeView.class, options))
						.readAttributes(args[1]);
			case HadoopFileAttributeViewImpl.NAME:
				return ((HadoopFileAttributeViewImpl) getFileAttributeView(
						path, HadoopFileAttributeView.class, options))
						.readAttributes(args[1]);
			default:
				throw new UnsupportedOperationException(String.format(
						"attributeview %s not supported", args[0]));
			}
		}
		// default to basic
		return ((BasicFileAttributeViewImpl) getFileAttributeView(path,
				BasicFileAttributeView.class, options))
				.readAttributes(attributes);
	}

	@Override
	public void setAttribute(Path path, String attribute, Object value,
			LinkOption... options) throws IOException {
		((HadoopFileAttributeViewImpl) getFileAttributeView(path,
				HadoopFileAttributeView.class, options)).setAttribute(
				attribute, value);

	}

}
