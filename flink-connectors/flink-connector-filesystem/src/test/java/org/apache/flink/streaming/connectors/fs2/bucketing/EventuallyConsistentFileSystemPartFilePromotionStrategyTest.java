/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.fs2.bucketing;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * This tests all methods in {@link EventuallyConsistentFileSystemPartFilePromotionStrategy}.
 */
public class EventuallyConsistentFileSystemPartFilePromotionStrategyTest {

	private static final String PART_FILE = "part-1-5abbf812-fae3-4fa1-b962-d37346ccd204";

	private static final String PENDING_SUFFIX = ".pending";

	private static final String FINAL_SUFFIX = ".final";

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	private FileSystem fs;

	private EventuallyConsistentFileSystemPartFilePromotionStrategy partFilePromotionStrategy;

	@Before
	public void setUp() {
		fs = FileSystem.getLocalFileSystem();

		partFilePromotionStrategy =
			new EventuallyConsistentFileSystemPartFilePromotionStrategy(
				PENDING_SUFFIX,
				FINAL_SUFFIX);
	}

	@Test
	public void testPromoteToPendingIdempotent() throws Exception {
		final Path partFile = new Path(folder.newFile(PART_FILE).toURI());
		partFilePromotionStrategy.promoteToPending(fs, partFile);
		partFilePromotionStrategy.promoteToPending(fs, partFile);
		assertTrue(fs.exists(partFile.suffix(PENDING_SUFFIX)));
		assertEquals(2, fs.listStatus(new Path(folder.getRoot().toURI())).length);
	}

	@Test
	public void testPromoteToFinalIdempotent() throws Exception {
		final Path partFile = new Path(folder.newFile(PART_FILE).toURI());
		partFilePromotionStrategy.promoteToFinal(fs, partFile);
		partFilePromotionStrategy.promoteToFinal(fs, partFile);
		assertTrue(fs.exists(partFile.suffix(FINAL_SUFFIX)));
		assertEquals(2, fs.listStatus(new Path(folder.getRoot().toURI())).length);
	}

	@Test
	public void testGetPendingPathFor() throws Exception {
		final Path pendingPath = partFilePromotionStrategy.getPendingPathFor(new Path("/path/to", PART_FILE));
		assertEquals("/path/to/" + PART_FILE + ".pending", pendingPath.toString());
	}

	@Test
	public void testGetInProgressPathFor() throws Exception {
		final Path inProgressPath = partFilePromotionStrategy.getInProgressPathFor(new Path("/path/to", PART_FILE));
		assertEquals("/path/to/" + PART_FILE, inProgressPath.toString());
	}

	@Test
	public void testReturnsDifferentPartFileSuffix() {
		final String partFileSuffix1 = partFilePromotionStrategy.getPartFileSuffix(42);
		final String partFileSuffix2 = partFilePromotionStrategy.getPartFileSuffix(42);
		assertNotEquals(partFileSuffix1, partFileSuffix2);
	}

}
