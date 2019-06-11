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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;

/**
 * Minimal implementation.
 */
public class MinimalFailoverTopology implements FailoverTopology {

	private final ExecutionGraph executionGraph;

	public MinimalFailoverTopology(final ExecutionGraph executionGraph) {
		this.executionGraph = requireNonNull(executionGraph);
	}

	@Override
	public Iterable<? extends FailoverVertex> getFailoverVertices() {
		return StreamSupport.stream(executionGraph.getAllExecutionVertices().spliterator(), false)
			.map(MinimalFailoverVertex::new)
			.collect(Collectors.toList());
	}

	@Override
	public boolean containsCoLocationConstraints() {
		return false;
	}

	static class MinimalFailoverVertex implements FailoverVertex {

		private final ExecutionVertex executionVertex;

		MinimalFailoverVertex(final ExecutionVertex executionVertex) {
			this.executionVertex = executionVertex;
		}

		@Override
		public ExecutionVertexID getExecutionVertexID() {
			return new ExecutionVertexID(executionVertex.getJobvertexId(), executionVertex.getParallelSubtaskIndex());
		}

		@Override
		public String getExecutionVertexName() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Iterable<? extends FailoverEdge> getInputEdges() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Iterable<? extends FailoverEdge> getOutputEdges() {
			throw new UnsupportedOperationException();
		}
	}
}
