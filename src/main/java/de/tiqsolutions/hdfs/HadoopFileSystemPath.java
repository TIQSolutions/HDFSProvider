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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchEvent.Modifier;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;

import org.apache.commons.lang.NullArgumentException;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;

class HadoopFileSystemPath implements Path {

	private final HadoopFileSystem fileSystem;
	private final URI base;

	public org.apache.hadoop.fs.Path getPath() {
		return new org.apache.hadoop.fs.Path(toAbsolutePath().toUri());
	}

	public HadoopFileSystemPath(HadoopFileSystem fileSystem, URI base) {
		if (fileSystem == null)
			throw new NullArgumentException("fileSystem");
		if (base == null)
			throw new NullArgumentException("base");
		this.fileSystem = fileSystem;
		this.base = base;
	}

	@Override
	public FileSystem getFileSystem() {
		return fileSystem;
	}

	@Override
	public boolean isAbsolute() {
		return base.isAbsolute();
	}

	@Override
	public Path getRoot() {
		return new HadoopFileSystemPath(fileSystem, fileSystem.getFileContext()
				.getDefaultFileSystem().getUri()).relativize(fileSystem
				.getPath("/"));
	}

	@Override
	public Path getFileName() {
		Path p = getParent();
		return p == null ? null : p.relativize(this);
	}

	@Override
	public Path getParent() {
		if (getNameCount() == 0)
			return null;
		URI p = base
				.resolve(base.getPath().endsWith(fileSystem.getSeparator()) ? ".."
						: ".");
		if (URI.create("").equals(p))
			return getRoot();
		return new HadoopFileSystemPath(fileSystem, p);
	}

	@Override
	public int getNameCount() {
		return Collections2.filter(
				Arrays.asList(base.normalize().getPath()
						.split(fileSystem.getSeparator())),
				new Predicate<String>() {

					@Override
					public boolean apply(String input) {
						return input != null && !input.isEmpty();
					}
				}).size();
	}

	@Override
	public Path getName(int index) {
		int c = getNameCount();
		if (index >= c || index < 0)
			return null;
		Path p = this;
		for (int i = 0; i < c - index - 1; i++) {
			p = p.getParent();
		}
		return p.getFileName();
	}

	@Override
	public Path subpath(int beginIndex, int endIndex) {
		Path r = null;
		int i = 0;
		for (Path p : this) {
			if (i++ == beginIndex)
				r = p;
			else if (i > beginIndex && i <= endIndex) {
				r = r.resolve(p);
			}
		}
		return r;
	}

	private boolean startsWith(Iterator<Path> i1, Iterator<Path> i2) {
		while (i1.hasNext() && i2.hasNext()) {
			if (!i1.next().equals(i2.next()))
				return false;
		}
		return !i2.hasNext();
	}

	@Override
	public boolean startsWith(Path other) {
		return startsWith(this.iterator(), other.iterator());

	}

	@Override
	public boolean startsWith(String other) {
		return startsWith(new HadoopFileSystemPath(fileSystem,
				URI.create(other)));
	}

	@Override
	public boolean endsWith(Path other) {
		return startsWith(this.descendingIterator(),
				((HadoopFileSystemPath) other).descendingIterator());
	}

	@Override
	public boolean endsWith(String other) {
		return endsWith(new HadoopFileSystemPath(fileSystem, URI.create(other)));
	}

	@Override
	public Path normalize() {
		return new HadoopFileSystemPath(fileSystem, base.normalize());
	}

	@Override
	public Path resolve(Path other) {
		return new HadoopFileSystemPath(fileSystem, base.resolve(other.toUri()));
	}

	@Override
	public Path resolve(String other) {
		return resolve(new HadoopFileSystemPath(fileSystem, URI.create(other)));

	}

	@Override
	public Path resolveSibling(Path other) {
		return getParent().resolve(other);
	}

	@Override
	public Path resolveSibling(String other) {
		return resolveSibling(new HadoopFileSystemPath(fileSystem,
				URI.create(other)));

	}

	@Override
	public Path relativize(Path other) {
		return new HadoopFileSystemPath(fileSystem, base.relativize(other
				.toUri()));
	}

	@Override
	public URI toUri() {
		return base;
	}

	@Override
	public Path toAbsolutePath() {
		if (isAbsolute())
			return this;
		return new HadoopFileSystemPath(fileSystem, fileSystem.getFileContext()
				.getDefaultFileSystem().getUri().resolve(base));
	}

	@Override
	public Path toRealPath(LinkOption... options) throws IOException {
		return toAbsolutePath();
	}

	@Override
	public File toFile() {
		throw new UnsupportedOperationException();
	}

	@Override
	public WatchKey register(WatchService watcher, Kind<?>[] events,
			Modifier... modifiers) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public WatchKey register(WatchService watcher, Kind<?>... events)
			throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<Path> iterator() {
		return getPathSegments().iterator();
	}

	Iterator<Path> descendingIterator() {
		return getPathSegments().descendingIterator();
	}

	Deque<Path> getPathSegments() {
		Deque<Path> paths = new ArrayDeque<>();
		Path root = getRoot();
		Path p = this;
		while (p != null && !p.equals(root)) {
			paths.push(p.getFileName());
			p = p.getParent();
		}
		return paths;
	}

	@Override
	public int compareTo(Path other) {
		if (!fileSystem.equals(other.getFileSystem()))
			throw new IllegalArgumentException();
		return base.compareTo(((HadoopFileSystemPath) other).base);
	}

	@Override
	public String toString() {
		return base.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((base == null) ? 0 : base.hashCode());
		result = prime * result
				+ ((fileSystem == null) ? 0 : fileSystem.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		HadoopFileSystemPath other = (HadoopFileSystemPath) obj;
		if (fileSystem == null) {
			if (other.fileSystem != null)
				return false;
		} else if (!fileSystem.equals(other.fileSystem))
			return false;
		return toAbsolutePath().toUri().equals(other.toAbsolutePath().toUri());
	}

}
