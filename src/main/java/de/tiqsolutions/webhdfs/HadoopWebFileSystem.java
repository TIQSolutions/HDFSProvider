package de.tiqsolutions.webhdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;

public class HadoopWebFileSystem extends AbstractFileSystem {
	public static final String SCHEME = "webhdfs";
	public static final String CONFIGURATION = "webhdfs-default.xml";
	private final WebHdfsFileSystem webHdfsFileSystem;

	public HadoopWebFileSystem(URI uri, Configuration conf)
			throws URISyntaxException, IOException {
		super(uri, "webhdfs", true, conf.getInt("dfs.http.port", 50070));
		this.webHdfsFileSystem = (WebHdfsFileSystem) FileSystem.get((URI) uri,
				(Configuration) conf);
	}

	public int getUriDefaultPort() {
		return this.webHdfsFileSystem.getConf().getInt("dfs.http.port", 50070);
	}

	public FsServerDefaults getServerDefaults() throws IOException {
		return this.webHdfsFileSystem.getServerDefaults(this.webHdfsFileSystem
				.getHomeDirectory());
	}

	public FSDataOutputStream createInternal(Path f, EnumSet<CreateFlag> flag,
			FsPermission absolutePermission, int bufferSize, short replication,
			long blockSize, Progressable progress,
			Options.ChecksumOpt checksumOpt, boolean createParent)
			throws AccessControlException, FileAlreadyExistsException,
			FileNotFoundException, ParentNotDirectoryException,
			UnsupportedFileSystemException, UnresolvedLinkException,
			IOException {
		return this.webHdfsFileSystem.create(f, absolutePermission, flag,
				bufferSize, replication, blockSize, progress);
	}

	public void mkdir(Path dir, FsPermission permission, boolean createParent)
			throws AccessControlException, FileAlreadyExistsException,
			FileNotFoundException, UnresolvedLinkException, IOException {
		Path parent = new Path(dir, new Path(".."));
		// URI p = dir.resolve(dir.getPath().endsWith(fileSystem.getSeparator())
		// ? "..": ".");
		if (!(createParent || this.webHdfsFileSystem.exists(parent))) {
			throw new FileNotFoundException(dir.getParent().getName());
		}
		this.webHdfsFileSystem.mkdirs(dir, permission);
	}

	public boolean delete(Path f, boolean recursive)
			throws AccessControlException, FileNotFoundException,
			UnresolvedLinkException, IOException {
		return this.webHdfsFileSystem.delete(f, recursive);
	}

	public FSDataInputStream open(Path f, int bufferSize)
			throws AccessControlException, FileNotFoundException,
			UnresolvedLinkException, IOException {
		return this.webHdfsFileSystem.open(f, bufferSize);
	}

	public boolean setReplication(Path f, short replication)
			throws AccessControlException, FileNotFoundException,
			UnresolvedLinkException, IOException {
		return this.webHdfsFileSystem.setReplication(f, replication);
	}

	public void renameInternal(Path src, Path dst)
			throws AccessControlException, FileAlreadyExistsException,
			FileNotFoundException, ParentNotDirectoryException,
			UnresolvedLinkException, IOException {
		this.webHdfsFileSystem.rename(src, dst);
	}

	public void setPermission(Path f, FsPermission permission)
			throws AccessControlException, FileNotFoundException,
			UnresolvedLinkException, IOException {
		this.webHdfsFileSystem.setPermission(f, permission);
	}

	public void setOwner(Path f, String username, String groupname)
			throws AccessControlException, FileNotFoundException,
			UnresolvedLinkException, IOException {
		this.webHdfsFileSystem.setOwner(f, username, groupname);
	}

	public void setTimes(Path f, long mtime, long atime)
			throws AccessControlException, FileNotFoundException,
			UnresolvedLinkException, IOException {
		this.webHdfsFileSystem.setTimes(f, mtime, atime);
	}

	public FileChecksum getFileChecksum(Path f) throws AccessControlException,
			FileNotFoundException, UnresolvedLinkException, IOException {
		return this.webHdfsFileSystem.getFileChecksum(f);
	}

	public FileStatus getFileStatus(Path f) throws AccessControlException,
			FileNotFoundException, UnresolvedLinkException, IOException {
		return this.webHdfsFileSystem.getFileStatus(f);
	}

	public BlockLocation[] getFileBlockLocations(Path f, long start, long len)
			throws AccessControlException, FileNotFoundException,
			UnresolvedLinkException, IOException {
		return this.getFileBlockLocations(f, start, len);
	}

	public FsStatus getFsStatus() throws AccessControlException,
			FileNotFoundException, IOException {
		return this.webHdfsFileSystem.getStatus();
	}

	public FileStatus[] listStatus(Path f) throws AccessControlException,
			FileNotFoundException, UnresolvedLinkException, IOException {
		return this.webHdfsFileSystem.listStatus(f);
	}

	public void setVerifyChecksum(boolean verifyChecksum)
			throws AccessControlException, IOException {
		this.webHdfsFileSystem.setVerifyChecksum(verifyChecksum);
	}
}
