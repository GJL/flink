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

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * test.
 */
public class ExecutionVertexIDTopologicalComparator implements Comparator<ExecutionVertexID> {

	private final JobGraph jobGraph;

	private final Map<JobVertexID, Integer> jobVertexToIndex;

	public ExecutionVertexIDTopologicalComparator(final JobGraph jobGraph) {
		this.jobGraph = jobGraph;
		this.jobVertexToIndex = new HashMap<>();
		initJobVertexToIndex();
	}

	private void initJobVertexToIndex() {
		int i = 0;
		for (final JobVertex jobVertex : jobGraph.getVerticesSortedTopologicallyFromSources()) {
			jobVertexToIndex.put(jobVertex.getID(), i);
			++i;
		}
	}

	@Override
	public int compare(final ExecutionVertexID executionVertex1, final ExecutionVertexID executionVertex2) {
		final JobVertexID jobVertexId1 = executionVertex1.getJobVertexId();
		final Integer vertex1Index = jobVertexToIndex.get(jobVertexId1);
		checkState(vertex1Index != null);

		final JobVertexID jobVertexId2 = executionVertex2.getJobVertexId();
		final Integer vertex2Index = jobVertexToIndex.get(jobVertexId2);
		checkState(vertex2Index != null);

		return Integer.compare(vertex1Index, vertex2Index);
	}
}
