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
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import de.tiqsolutions.webhdfs.HadoopWebFileSystem;

@RunWith(Parameterized.class)
public abstract class HadoopTestBase {
	private static URI HDFS_BASE_URI;
	private static URI WEBHDFS_BASE_URI;
	protected final URI BASE_URI;
	private static MiniDFSCluster hdfsCluster;

	@Parameters
	public static Iterable<String[]> data() {
		return Arrays.asList(new String[][] { { HadoopFileSystem.SCHEME },
				{ HadoopWebFileSystem.SCHEME } });
	}

	public HadoopTestBase(String base) {
		switch (base) {
		case HadoopFileSystem.SCHEME:
			BASE_URI = HDFS_BASE_URI;
			break;
		case HadoopWebFileSystem.SCHEME:
			BASE_URI = WEBHDFS_BASE_URI;
			break;
		default:
			throw new IllegalArgumentException(base);
		}
	}

	@BeforeClass
	public static void setUp() throws Exception {
		String key = "java.protocol.handler.pkgs";
		String newValue = "de.tiqsolutions";
		if (System.getProperty(key) != null) {
			String previousValue = System.getProperty(key);
			newValue = String.valueOf(newValue) + "|" + previousValue;
		}
		System.setProperty(key, newValue);
		File baseDir = new File("./target/hdfs/"
				+ HadoopTestBase.class.getName()).getAbsoluteFile();
		FileUtil.fullyDelete((File) baseDir);
		Configuration conf = new Configuration();
		conf.set("hdfs.minidfs.basedir", baseDir.getAbsolutePath());
		conf.setBoolean("dfs.webhdfs.enabled", true);
		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
		hdfsCluster = builder.build();
		hdfsCluster.waitActive();
		HDFS_BASE_URI = hdfsCluster.getURI();
		WEBHDFS_BASE_URI = new URI("webhdfs://"
				+ conf.get("dfs.namenode.http-address"));

		try (FileSystem fs = FileSystems.newFileSystem(HDFS_BASE_URI,
				System.getenv())) {

			Files.copy(Paths.get(HadoopTestBase.class.getResource("/test.csv")
					.toURI()), Paths.get(HDFS_BASE_URI.resolve("/test.csv")),
					StandardCopyOption.REPLACE_EXISTING);

		}

	}

	@AfterClass
	public static void tearDown() throws Exception {
		hdfsCluster.shutdown();
	}
}
