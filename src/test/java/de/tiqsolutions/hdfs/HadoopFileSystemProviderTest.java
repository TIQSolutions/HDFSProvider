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
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.spi.FileSystemProvider;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HadoopFileSystemProviderTest extends HadoopTestBase {
	private URI hdfsfile;

	@Before
	public void setup() throws URISyntaxException {
		this.hdfsfile = HDFS_BASE_URI.resolve("/test.csv");
	}

	@Test
	public void testService() {
		ServiceLoader<FileSystemProvider> loader = ServiceLoader
				.load(FileSystemProvider.class);
		boolean found = false;
		for (FileSystemProvider provider : loader) {
			if (!HadoopFileSystemProvider.class.isInstance(provider))
				continue;
			found = true;
			break;
		}
		Assert.assertTrue((boolean) found);
	}

	@Test
	public void testURI() throws IOException {
		try (FileSystem fs = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			Assert.assertTrue((boolean) HadoopFileSystem.class.isInstance(fs));
		}
	}

	@Test
	public void testGetFileSystem() throws IOException {
		try (FileSystem fs2 = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			FileSystem fs3 = FileSystems.getFileSystem(this.hdfsfile);
			Assert.assertSame(fs2, fs3);

		}
	}

	@Test(expected = FileSystemNotFoundException.class)
	public void testGetFileSystemBeforeNew() throws IOException {
		FileSystems.getFileSystem(this.hdfsfile);
	}

	@Test(expected = FileSystemAlreadyExistsException.class)
	public void testNewGetFileSystem() throws IOException {
		try (FileSystem fs = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			FileSystems.newFileSystem(this.hdfsfile, System.getenv());
		}
	}

	@Test
	public void testGetPath() throws IOException {
		try (FileSystem fs = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			Path path = fs.getPath(this.hdfsfile.toString(), new String[0]);
			Assert.assertEquals((Object) path.toUri(), (Object) this.hdfsfile);
		}
	}

	@Test
	public void testGetScheme() {
		Assert.assertEquals((Object) "hdfs",
				(Object) new HadoopFileSystemProvider().getScheme());
	}

	@Test
	public void testNewByteChannelPathSetOfQextendsOpenOptionFileAttributeOfQArray()
			throws IOException {
		try (FileSystem fs = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {

			FileSystemProvider provider = fs.provider();
			Path path = fs.getPath("/",
					String.format("%d", System.currentTimeMillis()));
			try (SeekableByteChannel channel = provider.newByteChannel(path,
					EnumSet.of(StandardOpenOption.CREATE_NEW,
							StandardOpenOption.WRITE), new FileAttribute[0])) {
				;

				channel.write(ByteBuffer.wrap("test".getBytes()));
			}
			Assert.assertTrue((boolean) Files.exists(path, new LinkOption[0]));

			try (SeekableByteChannel channel = provider.newByteChannel(path,
					EnumSet.of(StandardOpenOption.DELETE_ON_CLOSE,
							StandardOpenOption.READ), new FileAttribute[0])) {

				byte[] buf = new byte[20];
				ByteBuffer buffer = ByteBuffer.wrap(buf);
				buffer.clear();
				channel.read(buffer);
				int len = buffer.position();
				buffer.rewind();
				String s = new String(Arrays.copyOf(buf, len));
				Assert.assertEquals((Object) "test", (Object) s);

			}
			Assert.assertFalse((boolean) Files.exists(path, new LinkOption[0]));
		}
	}

	@Test
	public void testNewDirectoryStreamPathFilterOfQsuperPath()
			throws IOException {
		try (FileSystem fileSystem = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			int found;
			FileSystemProvider provider = fileSystem.provider();
			Path path = fileSystem.getPath("/", new String[0]);
			found = 0;
			try (DirectoryStream<Path> stream = provider.newDirectoryStream(
					path, new DirectoryStream.Filter<Path>() {
						@Override
						public boolean accept(Path entry) throws IOException {
							return true;
						}

					})) {
				for (Path p : stream) {
					System.out.println(p.toString());
					if (!Files.isRegularFile(p, new LinkOption[0]))
						continue;
					++found;
				}

			}

			Assert.assertTrue((boolean) (found > 0));
		}
	}

	@Test
	public void testCreateDirectoryPathFileAttributeOfQArray()
			throws IOException {

		try (FileSystem fileSystem = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			FileSystemProvider provider = fileSystem.provider();
			Path path = fileSystem.getPath("/",
					String.format("%d", System.currentTimeMillis()));
			provider.createDirectory(path, new FileAttribute[0]);
			Assert.assertTrue((boolean) Files.exists(path, new LinkOption[0]));
			Assert.assertTrue((boolean) Files.isDirectory(path,
					new LinkOption[0]));
			provider.delete(path);

		}
	}

	@Test
	public void testDeletePath() throws IOException {
		try (FileSystem fileSystem = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			Path p = fileSystem.getPath("/",
					String.format("%d", System.currentTimeMillis()));
			HashSet<PosixFilePermission> permissions = new HashSet<PosixFilePermission>();
			permissions.add(PosixFilePermission.OWNER_WRITE);
			permissions.add(PosixFilePermission.OWNER_READ);
			permissions.add(PosixFilePermission.OWNER_EXECUTE);
			permissions.add(PosixFilePermission.OTHERS_WRITE);
			permissions.add(PosixFilePermission.OTHERS_READ);
			permissions.add(PosixFilePermission.OTHERS_EXECUTE);
			FileSystemProvider provider = fileSystem.provider();
			provider.createDirectory(p,
					PosixFilePermissions.asFileAttribute(permissions));
			Assert.assertTrue((boolean) Files.exists(p, new LinkOption[0]));
			Assert.assertTrue((boolean) Files.isDirectory(p, new LinkOption[0]));
			provider.delete(p);
			Assert.assertTrue((boolean) Files.notExists(p, new LinkOption[0]));

		}
	}

	@Test
	public void testCopyToLocal() throws IOException, InterruptedException,
			URISyntaxException {
		try (FileSystem fileSystem = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			FileSystemProvider provider = fileSystem.provider();
			Path oldPath = fileSystem.getPath(this.hdfsfile.getPath(),
					new String[0]);
			Path newPath = Paths.get(this.getClass().getResource("/").toURI())
					.resolve("test2.csv");
			Files.createDirectories(newPath.getParent(), new FileAttribute[0]);
			provider.copy(oldPath, newPath, StandardCopyOption.COPY_ATTRIBUTES,
					StandardCopyOption.REPLACE_EXISTING);
			Assert.assertTrue((boolean) Files
					.exists(newPath, new LinkOption[0]));
		}
	}

	@Test
	public void testCopyFromLocal() throws IOException, InterruptedException,
			URISyntaxException {
		try (FileSystem fileSystem = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			FileSystemProvider provider = fileSystem.provider();
			Path oldPath = Paths.get(this.getClass().getResource("/test.csv")
					.toURI());
			Path newPath = fileSystem.getPath(this.hdfsfile.getPath(),
					new String[0]);
			Files.createDirectories(newPath.getParent(), new FileAttribute[0]);
			provider.copy(oldPath, newPath, StandardCopyOption.COPY_ATTRIBUTES,
					StandardCopyOption.REPLACE_EXISTING);
			Assert.assertTrue((boolean) Files
					.exists(newPath, new LinkOption[0]));

		}
	}

	@Test
	public void testCopyRemote() throws IOException {
		try (FileSystem fileSystem = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			FileSystemProvider provider = fileSystem.provider();
			Path path1 = fileSystem.getPath(this.hdfsfile.getPath(),
					new String[0]);
			Path path2 = fileSystem.getPath(this.hdfsfile
					.resolve("./test2.csv").getPath(), new String[0]);
			provider.deleteIfExists(path2);
			provider.copy(path1, path2, new CopyOption[] {
					StandardCopyOption.REPLACE_EXISTING,
					HadoopCopyOption.REMOTE_COPY });
			Assert.assertTrue((boolean) Files.exists(path2, new LinkOption[0]));
			Assert.assertFalse((boolean) Files.isDirectory(path2,
					new LinkOption[0]));
			Assert.assertTrue((boolean) Files.isRegularFile(path2,
					new LinkOption[0]));

		}
	}

	@Test
	public void testMovePathPathCopyOptionArray() throws IOException {
		try (FileSystem fileSystem = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {

			FileSystemProvider provider = fileSystem.provider();
			Path path1 = fileSystem.getPath(this.hdfsfile.getPath(),
					new String[0]);
			Path path2 = fileSystem.getPath(this.hdfsfile
					.resolve("./test2.csv").getPath(), new String[0]);
			provider.deleteIfExists(path2);
			provider.copy(path1, path2, StandardCopyOption.REPLACE_EXISTING);
			Assert.assertTrue((boolean) Files.exists(path2, new LinkOption[0]));
			Assert.assertFalse((boolean) Files.isDirectory(path2,
					new LinkOption[0]));
			Assert.assertTrue((boolean) Files.isRegularFile(path2,
					new LinkOption[0]));
			Path path3 = fileSystem.getPath(this.hdfsfile
					.resolve("./test3.csv").getPath(), new String[0]);
			provider.move(path2, path3, StandardCopyOption.REPLACE_EXISTING);
			Assert.assertFalse((boolean) Files.exists(path2, new LinkOption[0]));
			Assert.assertTrue((boolean) Files.exists(path3, new LinkOption[0]));
			Assert.assertFalse((boolean) Files.isDirectory(path3,
					new LinkOption[0]));
			Assert.assertTrue((boolean) Files.isRegularFile(path3,
					new LinkOption[0]));
			provider.delete(path3);

		}
	}

	@Test
	public void testIsSameFilePathPath() throws IOException {
		try (FileSystem fileSystem = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			Path path1 = fileSystem.getPath(this.hdfsfile.getPath(),
					new String[0]);
			Path path2 = fileSystem.getPath(this.hdfsfile
					.resolve("./test2.csv").getPath(), new String[0]);
			Assert.assertFalse((boolean) Files.isSameFile(path1, path2));
			Assert.assertTrue((boolean) Files.isSameFile(path1, path1));

		}
	}

	@Test
	public void testIsHiddenPath() throws IOException {
		try (FileSystem fileSystem = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			Assert.assertFalse((boolean) Files.isHidden(fileSystem.getPath(
					this.hdfsfile.getPath(), new String[0])));

		}
	}

	@Test
	public void testGetFileStorePath() throws IOException {
		try (FileSystem fileSystem = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			Assert.assertNotNull((Object) fileSystem.provider().getFileStore(
					fileSystem.getPath("/", new String[0])));

		}
	}

	@Test
	public void testCheckAccessPathAccessModeArray() throws IOException {
		try (FileSystem fileSystem = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			fileSystem.provider().checkAccess(
					fileSystem.getPath("/", new String[0]), AccessMode.READ);

		}
	}

	@Test
	public void testGetFileAttributeViewPathClassOfVLinkOptionArray()
			throws IOException {
		try (FileSystem fileSystem = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			HadoopFileAttributeView view = (HadoopFileAttributeView) fileSystem
					.provider().getFileAttributeView(
							fileSystem.getPath("/", new String[0]),
							HadoopFileAttributeView.class, new LinkOption[0]);
			Assert.assertNotNull((Object) view);
			HadoopFileAttributes attributes = view.readAttributes();
			Assert.assertNotNull((Object) attributes);
			Assert.assertTrue((boolean) attributes.isDirectory());
			Assert.assertFalse((boolean) attributes.isRegularFile());
			Assert.assertFalse((boolean) attributes.isOther());
			Assert.assertFalse((boolean) attributes.isSymbolicLink());
			Assert.assertFalse((boolean) attributes.isHidden());

		}
	}

	@Test
	public void testReadAttributesPathClassOfALinkOptionArray()
			throws IOException {
		try (FileSystem fileSystem = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			BasicFileAttributes attributes = fileSystem.provider()
					.readAttributes(fileSystem.getPath("/", new String[0]),
							BasicFileAttributes.class, new LinkOption[0]);
			Assert.assertTrue((boolean) Boolean.TRUE.equals(attributes
					.isDirectory()));
			Assert.assertTrue((boolean) Boolean.FALSE.equals(attributes
					.isSymbolicLink()));
			Assert.assertTrue((boolean) Boolean.FALSE.equals(attributes
					.isRegularFile()));
			Assert.assertTrue((boolean) Boolean.FALSE.equals(attributes
					.isOther()));
			PosixFileAttributes posixAttributes = (PosixFileAttributes) fileSystem
					.provider().readAttributes(
							fileSystem.getPath("/", new String[0]),
							PosixFileAttributes.class, new LinkOption[0]);
			Assert.assertNotNull(posixAttributes.permissions());
			Assert.assertTrue((boolean) (posixAttributes.permissions().size() > 0));
			Assert.assertNotNull((Object) posixAttributes.owner());
			Assert.assertNotNull((Object) posixAttributes.group());
			HadoopFileAttributes hdfsAttributes = (HadoopFileAttributes) fileSystem
					.provider().readAttributes(
							fileSystem.getPath(this.hdfsfile.getPath(),
									new String[0]), HadoopFileAttributes.class,
							new LinkOption[0]);
			Assert.assertTrue((boolean) Boolean.FALSE.equals(hdfsAttributes
					.isHidden()));
			Assert.assertTrue((boolean) (hdfsAttributes.getBlockSize() > 0));
			Assert.assertTrue((boolean) (hdfsAttributes.getReplication() > 0));

		}
	}

	@Test
	public void testReadAttributesPathStringLinkOptionArray()
			throws IOException {
		try (FileSystem fileSystem = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			Map<String, Object> attributes = fileSystem.provider()
					.readAttributes(fileSystem.getPath("/", new String[0]),
							"isSymbolicLink,isRegularFile,isDirectory,isOther",
							new LinkOption[0]);
			Assert.assertTrue((boolean) Boolean.TRUE.equals(attributes
					.get("isDirectory")));
			Assert.assertTrue((boolean) Boolean.FALSE.equals(attributes
					.get("isSymbolicLink")));
			Assert.assertTrue((boolean) Boolean.FALSE.equals(attributes
					.get("isRegularFile")));
			Assert.assertTrue((boolean) Boolean.FALSE.equals(attributes
					.get("isOther")));
			attributes = fileSystem.provider().readAttributes(
					fileSystem.getPath("/", new String[0]), "posix:*",
					new LinkOption[0]);
			Assert.assertNotNull((Object) attributes.get("permissions"));
			Assert.assertTrue((boolean) (((Set<?>) attributes
					.get("permissions")).size() > 0));
			Assert.assertNotNull((Object) attributes.get("owner"));
			Assert.assertNotNull((Object) attributes.get("group"));
			attributes = fileSystem.provider().readAttributes(
					fileSystem.getPath(this.hdfsfile.getPath(), new String[0]),
					"hdfs:*", new LinkOption[0]);
			Assert.assertTrue((boolean) Boolean.FALSE.equals(attributes
					.get("isHidden")));
			Assert.assertTrue((boolean) ((Long) attributes.get("blockSize") > 0));
			Assert.assertTrue((boolean) ((Short) attributes.get("replication") > 0));

		}
	}

	@Test
	public void testSetAttributePathStringObjectLinkOptionArray()
			throws IOException {
		try (FileSystem fileSystem = FileSystems.newFileSystem(this.hdfsfile,
				System.getenv())) {
			FileSystemProvider provider = fileSystem.provider();
			Path path = fileSystem.getPath("/",
					String.format("%d/", System.currentTimeMillis()));
			provider.createDirectory(path, new FileAttribute[0]);
			path = path.resolve("test.csv");
			SeekableByteChannel c = provider.newByteChannel(path, EnumSet.of(
					StandardOpenOption.WRITE, StandardOpenOption.CREATE),
					new FileAttribute[0]);
			c.write(ByteBuffer.wrap("Test".getBytes()));
			provider.setAttribute(path, "hdfs:replication", (short) 5,
					new LinkOption[0]);
			HadoopFileAttributeView view = (HadoopFileAttributeView) provider
					.getFileAttributeView(path, HadoopFileAttributeView.class,
							new LinkOption[0]);
			Assert.assertEquals((long) 5, (long) view.readAttributes()
					.getReplication());
			provider.delete(path);
			provider.delete(path.getParent());

		}
	}
}
