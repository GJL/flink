/*
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
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

class ExecutionVertexVersioner {

	private final Map<ExecutionVertexID, Long> executionVertexToVersion = new HashMap<>();

	public ExecutionVertexVersion recordModification(final ExecutionVertexID executionVertexId) {
		final Long newVersion = executionVertexToVersion.merge(executionVertexId, 1L, Long::sum);
		return new ExecutionVertexVersion(executionVertexId, newVersion);
	}

	public boolean isModified(final ExecutionVertexVersion executionVertexVersion) {
		final Long currentVersion = executionVertexToVersion.get(executionVertexVersion.getExecutionVertexId());
		Preconditions.checkState(currentVersion != null);
		return currentVersion != executionVertexVersion.getVersion();
	}

}
