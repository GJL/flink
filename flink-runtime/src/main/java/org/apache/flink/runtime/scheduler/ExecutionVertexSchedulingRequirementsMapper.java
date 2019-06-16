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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.Collections;

final class ExecutionVertexSchedulingRequirementsMapper {

	public static ExecutionVertexSchedulingRequirements from(final ExecutionVertex executionVertex) {

		final ExecutionVertexID executionVertexId = new ExecutionVertexID(
			executionVertex.getJobVertex().getJobVertexId(),
			executionVertex.getParallelSubtaskIndex());

		final AllocationID latestPriorAllocation = executionVertex.getLatestPriorAllocation();
		final SlotSharingGroup slotSharingGroup = executionVertex.getJobVertex().getSlotSharingGroup();

		return new ExecutionVertexSchedulingRequirements.Builder()
			.withExecutionVertexId(executionVertexId)
			.withPreviousAllocationId(latestPriorAllocation)
			.withSlotSharingGroupId(slotSharingGroup == null ? null : slotSharingGroup.getSlotSharingGroupId())
			.withCoLocationConstraint(executionVertex.getLocationConstraint())
			// TODO: fix preferred locations
			.withPreferredLocations(Collections.emptyList()).build();
	}

	private ExecutionVertexSchedulingRequirementsMapper() {
	}
}
