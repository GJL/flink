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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import static org.apache.flink.util.Preconditions.checkArgument;

//TODO: finish javadoc

/**
 * A {@link PartFilePromotionStrategy} to be used with file systems that provide POSIX compliant
 * consistency.
 * <p>
 * <p>This strategy reflects a part file's state in its name.
 * Hence, this strategy requires renames of files in the file system to be immediately visible to
 * all clients.</p>
 */
@Internal
class ConsistentFileSystemPartFilePromotionStrategy implements PartFilePromotionStrategy {

	private static final Logger LOG = LoggerFactory.getLogger(ConsistentFileSystemPartFilePromotionStrategy.class);

	private final String inProgressSuffix;
	private final String inProgressPrefix;

	private final String pendingSuffix;
	private final String pendingPrefix;

	ConsistentFileSystemPartFilePromotionStrategy(
		String inProgressSuffix,
		String inProgressPrefix,
		String pendingSuffix,
		String pendingPrefix) {

		checkArgument(
			StringUtils.isEmpty(inProgressPrefix) || !StringUtils.isWhitespace(inProgressPrefix),
			"inProgressPrefix must not be whitespace");
		checkArgument(
			StringUtils.isEmpty(inProgressSuffix) || !StringUtils.isWhitespace(inProgressSuffix),
			"inProgressPrefix must not be whitespace");
		checkArgument(
			StringUtils.isEmpty(pendingSuffix) || !StringUtils.isWhitespace(pendingSuffix),
			"pendingSuffix must not be whitespace");
		checkArgument(
			StringUtils.isEmpty(pendingPrefix) || !StringUtils.isWhitespace(pendingPrefix),
			"pendingPrefix must not be whitespace");
		checkArgument(
			!(inProgressPrefix.equals(pendingPrefix) && inProgressSuffix.equals(pendingSuffix)),
			"(inProgressPrefix, inProgressSuffix) must not be equal to (pendingPrefix, pendingSuffix)");

		this.inProgressSuffix = inProgressSuffix;
		this.inProgressPrefix = inProgressPrefix;
		this.pendingSuffix = pendingSuffix;
		this.pendingPrefix = pendingPrefix;
	}

	@Override
	public void promoteToPending(FileSystem fs, Path partFile) throws IOException {
		final Path inProgressPath = getInProgressPathFor(partFile);
		final Path pendingPath = getPendingPathFor(partFile);
		LOG.debug("Moving in-progress file {} to pending file {}.", inProgressPath, pendingPath);
		fs.rename(inProgressPath, pendingPath);
	}

	@Override
	public void promoteToFinal(FileSystem fs, Path partFile) throws IOException {
		final Path inProgressPath = getInProgressPathFor(partFile);
		final Path pendingPath = getPendingPathFor(partFile);

		if (fs.exists(pendingPath)) {
			LOG.debug("In-progress file {} has been moved to pending after checkpoint, moving to " +
				"final location.", partFile);
			// has been moved to pending in the mean time, rename to final location
			fs.rename(pendingPath, partFile);
		} else if (fs.exists(inProgressPath)) {
			LOG.debug("In-progress file {} is still in-progress, moving to final location.", partFile);
			// it was still in progress, rename to final path
			fs.rename(inProgressPath, partFile);
		} else if (fs.exists(partFile)) {
			LOG.debug("Part file has already been moved to final location {}.", partFile);
		} else {
			LOG.warn("Invalid state on file system detected. " +
					"Expected to find one of the following files: {} but did not find any.",
				new HashSet<>(Arrays.asList(partFile, inProgressPath, pendingPath)));
		}
	}

	@Override
	public Path getPendingPathFor(Path partFile) {
		return new Path(partFile.getParent(), pendingPrefix + partFile.getName()).suffix(pendingSuffix);
	}

	@Override
	public Path getInProgressPathFor(Path partFile) {
		return new Path(partFile.getParent(), inProgressPrefix + partFile.getName()).suffix(inProgressSuffix);
	}

	@Override
	public String getPartFileSuffix(int partCount) {
		return Integer.toString(partCount);
	}

}
