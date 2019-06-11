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

import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverTopology;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverVertex;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A FailoverStrategy that always requires to restart all tasks.
 */
public class RestartAllTasksFailoverStrategy implements FailoverStrategy {

	private Set<ExecutionVertexID> allExecutionVertices;

	public RestartAllTasksFailoverStrategy(final FailoverTopology failoverTopology) {
		Objects.requireNonNull(failoverTopology);
		this.allExecutionVertices = Collections.unmodifiableSet(
			failoverVertexStream(failoverTopology)
				.map(FailoverVertex::getExecutionVertexID)
				.collect(Collectors.toSet()));
	}

	private static Stream<? extends FailoverVertex> failoverVertexStream(final FailoverTopology failoverTopology) {
		return StreamSupport.stream(failoverTopology.getFailoverVertices().spliterator(), false);
	}

	@Override
	public Set<ExecutionVertexID> getTasksNeedingRestart(
			final ExecutionVertexID executionVertexId,
			final Throwable cause) {
		return allExecutionVertices;
	}

	/**
	 * Factory for {@link RestartAllTasksFailoverStrategy}.
	 */
	public static class Factory implements FailoverStrategy.Factory {

		@Override
		public FailoverStrategy create(final FailoverTopology topology) {
			return new RestartAllTasksFailoverStrategy(topology);
		}
	}
}
