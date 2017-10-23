package org.apache.flink.hdfstests;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.util.OperatingSystem;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.VersionInfo;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests methods
 * {@link HadoopFileSystem#truncate(Path, long)} and {@link HadoopFileSystem#recoverLease(Path)}
 */
public class HadoopFileSystemTest {

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	private Path hdfsRootPath;

	private FileSystem fs;

	@Before
	public void setUp() throws Exception {
		Assume.assumeTrue(
			"HDFS cluster cannot be started on Windows without extensions.",
			!OperatingSystem.isWindows());

		final File tempDir = folder.newFolder();

		final Configuration hadoopConf = new Configuration();
		hadoopConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tempDir.getAbsolutePath());

		final MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hadoopConf);
		final MiniDFSCluster hdfsCluster = builder.build();

		hdfsRootPath = new Path("hdfs://" + hdfsCluster.getURI().getHost() + ":"
			+ hdfsCluster.getNameNodePort() + "/");

		fs = hdfsRootPath.getFileSystem();
		assertEquals("Must be HadoopFileSystem", HadoopFileSystem.class, fs.getClass());
	}

	@Test
	public void testTruncateHadoop27() throws Exception {
		Assume.assumeTrue("Truncate method is supported from Hadoop 2.7 and greater", isHadoopVersionGreaterThanEquals(2, 7));

		final Path fileToTruncate = new Path(hdfsRootPath, "truncateme");
		try (FSDataOutputStream fsDataOutputStream = fs.create(
			fileToTruncate,
			FileSystem.WriteMode.OVERWRITE)) {
			fsDataOutputStream.write("hello".getBytes(StandardCharsets.UTF_8));
		}

		assertTrue(fs.isTruncateSupported());

		final int expectedLength = 2;
		fs.truncate(fileToTruncate, expectedLength);
		assertEquals(expectedLength, fs.getFileStatus(fileToTruncate).getLen());
	}

	@Test
	public void testTruncateNotSupportedBeforeHadoop27() throws Exception {
		Assume.assumeFalse(
			"Truncate method is supported from Hadoop 2.7 and greater but we want to fail when calling truncate",
			isHadoopVersionGreaterThanEquals(2, 7));

		final Path fileToTruncate = new Path(hdfsRootPath, "truncateme");
		try (FSDataOutputStream fsDataOutputStream = fs.create(
			fileToTruncate,
			FileSystem.WriteMode.OVERWRITE)) {
			fsDataOutputStream.write("hello".getBytes(StandardCharsets.UTF_8));
		}

		assertFalse(fs.isTruncateSupported());

		try {
			fs.truncate(fileToTruncate, 2);
			fail("Truncate should not be supported before Hadoop 2.7");
		} catch (UnsupportedOperationException e) {
			assertEquals("Hadoop " +
				VersionInfo.getVersion() + " does not support truncate.", e.getMessage());
		}
	}

	@Test
	public void testRecoverLeaseShouldFailIfFileNotExists() throws Exception {
		try {
			final String randomFileName = RandomStringUtils.randomAlphabetic(21);
			fs.recoverLease(new Path(hdfsRootPath, randomFileName));
			fail("RecoverLease should fail if file does not exist");
		} catch (FileNotFoundException e) {
			assertTrue("Exception message did not match, was: " + e.getMessage(),
				e.getMessage().contains("File does not exist"));
		}
	}

	private static boolean isHadoopVersionGreaterThanEquals(int major, int minor) {
		final String hadoopVersion = VersionInfo.getVersion();
		final int[] versions = Arrays
			.stream(hadoopVersion.split("\\."))
			.mapToInt(Integer::parseInt)
			.toArray();

		if (versions.length != 3) {
			fail("Unexpected hadoop version format: " + hadoopVersion);
		}
		return versions[0] >= major && versions[1] >= minor;
	}

}
