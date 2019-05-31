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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A dumb {@link ExecutionSlotAllocator}.
 */
public class DumbExecutionSlotAllocator implements ExecutionSlotAllocator {

	private final SlotProvider slotProvider;

	private final Map<ExecutionVertexID, SlotRequestId> slotRequests;

	public DumbExecutionSlotAllocator(final SlotProvider slotProvider) {
		this.slotProvider = requireNonNull(slotProvider);
		this.slotRequests = new HashMap<>();
	}

	@Override
	public Collection<SlotExecutionVertexAssignment> allocateSlotsFor(final Collection<ExecutionVertexSchedulingRequirements> requirements) {
		return requirements.stream()
			.map(this::createSlotExecutionVertexAssignment)
			.collect(Collectors.toList());
	}

	private SlotExecutionVertexAssignment createSlotExecutionVertexAssignment(final ExecutionVertexSchedulingRequirements requirement) {
		final CompletableFuture<LogicalSlot> logicalSlotFuture = allocateSlot(requirement);

		final ExecutionVertexID executionVertexId = requirement.getExecutionVertexId();
		logicalSlotFuture.whenComplete(removeSlotRequest(executionVertexId));

		return new SlotExecutionVertexAssignment(executionVertexId, logicalSlotFuture);
	}

	private CompletableFuture<LogicalSlot> allocateSlot(final ExecutionVertexSchedulingRequirements requirement) {
		final ExecutionVertexID executionVertexId = requirement.getExecutionVertexId();
		final SlotProfile slotProfile = new SlotProfile(
			requirement.getResourceProfile(),
			Collections.emptyList(),
			Collections.emptyList());
		final SlotRequestId slotRequestId = new SlotRequestId();

		final SlotRequestId previousSlotRequestId = slotRequests.put(executionVertexId, slotRequestId);
		checkState(previousSlotRequestId == null, "Concurrent slot requests for execution vertex %s", executionVertexId);

		return slotProvider.allocateSlot(
			slotRequestId,
			new ScheduledUnit(
				null,
				executionVertexId.getJobVertexId(),
				null,
				null),
			slotProfile,
			true,
			Time.seconds(300));
	}

	private BiConsumer<LogicalSlot, Throwable> removeSlotRequest(final ExecutionVertexID executionVertexId) {
		return (logicalSlot, throwable) -> slotRequests.remove(executionVertexId);
	}

	@Override
	public void cancel(final ExecutionVertexID executionVertexId) {
		final SlotRequestId slotRequestId = slotRequests.get(executionVertexId);
		slotProvider.cancelSlotRequest(slotRequestId, null, new RuntimeException("Slot request cancelled"));
	}

	@Override
	public CompletableFuture<Void> stop() {
		return CompletableFuture.completedFuture(null);
	}
}
