package org.apache.flink.streaming.connectors.fs2.bucketing;


import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This tests all methods of {@link ConsistentFileSystemPartFilePromotionStrategy}.
 */
public class ConsistentFileSystemPartFilePromotionStrategyTest {

	private static final String IN_PROGRESS_SUFFIX = ".in-progress";

	private static final String IN_PROGRESS_PREFIX = "_";

	private static final String PENDING_SUFFIX = ".pending";

	private static final String PENDING_PREFIX = "";

	private static final String PART_FILE = "part-1-1";

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	private FileSystem fs;

	private ConsistentFileSystemPartFilePromotionStrategy partFilePromotionStrategy;

	@Before
	public void setUp() throws Exception {
		fs = FileSystem.getLocalFileSystem();

		partFilePromotionStrategy = new ConsistentFileSystemPartFilePromotionStrategy(
			IN_PROGRESS_SUFFIX,
			IN_PROGRESS_PREFIX,
			PENDING_SUFFIX,
			PENDING_PREFIX
		);
	}

	@Test
	public void testPromoteToPending() throws Exception {
		folder.newFile(IN_PROGRESS_PREFIX + PART_FILE + IN_PROGRESS_SUFFIX);

		final Path partFile = new Path(folder.newFile(PART_FILE).toURI());
		partFilePromotionStrategy.promoteToPending(fs, partFile);

		final Path pendingPath = partFilePromotionStrategy.getPendingPathFor(partFile);
		assertTrue("Expected file " + pendingPath + " to exist", fs.exists(pendingPath));
	}

	@Test
	public void testPromotePendingToFinal() throws Exception {
		final File pendingFile = folder.newFile(PENDING_PREFIX + PART_FILE + PENDING_SUFFIX);

		final Path partFile = new Path(new File(pendingFile.getParentFile(), "part-1-1").toURI());
		partFilePromotionStrategy.promoteToFinal(fs, partFile);

		assertTrue("Expected file " + partFile + " to exist", fs.exists(partFile));
	}

	@Test
	public void testPromoteInProgressToFinal() throws Exception {
		final File inProgressFile = folder.newFile(IN_PROGRESS_PREFIX + PART_FILE + IN_PROGRESS_SUFFIX);

		final Path partFile = new Path(new File(inProgressFile.getParentFile(), "part-1-1").toURI());
		partFilePromotionStrategy.promoteToFinal(fs, partFile);

		assertTrue("Expected file " + partFile + " to exist", fs.exists(partFile));
	}

	@Test
	public void testPromoteFinalToFinal() throws Exception {
		final Path partFile = new Path(folder.newFile(PART_FILE).toURI());
		partFilePromotionStrategy.promoteToFinal(fs, partFile);

		assertTrue("Expected file " + partFile + " to exist", fs.exists(partFile));
	}

	@Test
	public void testGetPendingPathFor() {
		final Path partFile = new Path("/path/to/part-1-1");
		final Path pendingPath = partFilePromotionStrategy.getPendingPathFor(partFile);
		assertEquals(
			"/path/to/" + PENDING_PREFIX + "part-1-1" + PENDING_SUFFIX,
			pendingPath.toString());
	}

	@Test
	public void testGetInProgressPathFor() {
		final Path partFile = new Path("/path/to/part-1-1");
		final Path pendingPath = partFilePromotionStrategy.getInProgressPathFor(partFile);
		assertEquals(
			"/path/to/" + IN_PROGRESS_PREFIX + "part-1-1" + IN_PROGRESS_SUFFIX,
			pendingPath.toString());
	}

	@Test
	public void testReturnsNumericPartFileSuffix() {
		assertEquals("42", partFilePromotionStrategy.getPartFileSuffix(42));
	}

}
