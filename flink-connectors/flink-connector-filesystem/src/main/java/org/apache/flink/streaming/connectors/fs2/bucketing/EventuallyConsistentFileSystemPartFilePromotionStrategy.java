/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.fs2.bucketing;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 *
 */
class EventuallyConsistentFileSystemPartFilePromotionStrategy implements PartFilePromotionStrategy {

	/** The suffix for <code>pending</code> flag files **/
	private final String pendingSuffix;

	/** The suffix for <code>final</code> flag files **/
	private final String finalSuffix;

	EventuallyConsistentFileSystemPartFilePromotionStrategy(String pendingSuffix, String finalSuffix) {
		checkArgument(!StringUtils.isBlank(finalSuffix), "finalSuffix must not be blank");
		checkArgument(!StringUtils.isBlank(pendingSuffix), "pendingSuffix must not be blank");
		checkArgument(!pendingSuffix.equals(finalSuffix), "pendingSuffix must not be equal to finalSuffix");

		this.pendingSuffix = pendingSuffix;
		this.finalSuffix = finalSuffix;
	}

	@Override
	public void promoteToPending(FileSystem fs, Path partFile) throws IOException {
		fs.create(getPendingPathFor(partFile), FileSystem.WriteMode.OVERWRITE).close();
	}

	@Override
	public void promoteToFinal(FileSystem fs, Path partFile) throws Exception {
		fs.create(getFinalPathFor(partFile), FileSystem.WriteMode.OVERWRITE).close();
	}

	private Path getFinalPathFor(Path f) {
		return new Path(f.getParent(), f.getName() + finalSuffix);
	}

	@Override
	public Path getPendingPathFor(Path partFile) {
		return new Path(partFile.getParent(), partFile.getName() + pendingSuffix);
	}

	@Override
	public Path getInProgressPathFor(Path partFile) {
		return partFile;
	}

	@Override
	public String getPartFileSuffix(int partCount) {
		return UUID.randomUUID().toString();
	}

}
