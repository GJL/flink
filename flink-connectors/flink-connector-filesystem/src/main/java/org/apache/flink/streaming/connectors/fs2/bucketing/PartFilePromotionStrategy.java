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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

/**
 * A {@link PartFilePromotionStrategy} is used by the {@link BucketingSink} to promote part files
 * to different states. Exchangeable strategies are required due to different consistency guarantees
 * across file systems.
 *
 * @see EventuallyConsistentFileSystemPartFilePromotionStrategy
 * @see ConsistentFileSystemPartFilePromotionStrategy
 */
@Internal
interface PartFilePromotionStrategy {

	/**
	 * Promotes a part file to the <code>pending</code> state.
	 * @param fs File system containing the part files.
	 * @param partFile Part file to be promoted.
	 * @throws Exception
	 */
	void promoteToPending(FileSystem fs, Path partFile) throws Exception;

	/**
	 * Promotes a part file to the <i>final</i> state.
	 * @param fs File system containing the part files.
	 * @param partFile Part file to be promoted.
	 * @throws Exception
	 */
	void promoteToFinal(FileSystem fs, Path partFile) throws Exception;

	/**
	 * Returns the path for a specified part file that indicates that the part file is in
	 * <code>pending</code> state.
	 */
	Path getPendingPathFor(Path partFile);

	/**
	 * Returns the path to the <code>in-progress</code> part file. This will be the path used to
	 * write data into.
	 */
	Path getInProgressPathFor(Path partFile);

	/**
	 * Returns the part file suffix that this strategy chooses given a part count.
	 * @param partCount The number of part files already in the bucket.
	 */
	String getPartFileSuffix(int partCount);

}
