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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link DefaultExecutionSlotAllocator}.
 */
public class DefaultExecutionSlotAllocatorTest2 {

	private static final Time SLOT_ALLOCATION_TIMEOUT = Time.seconds(300);

	@Test
	public void producersAreAssignedToSlotsBeforeConsumers() {
		final ExecutionVertexID producerId = new ExecutionVertexID(new JobVertexID(), 0);
		final ExecutionVertexID consumerId = new ExecutionVertexID(new JobVertexID(), 0);

		final TestingInputLocationRetriever inputLocationRetriever = new TestingInputLocationRetriever.Builder()
			.connectConsumerToProducer(consumerId, producerId)
			.build();

		final DefaultExecutionSlotAllocator executionSlotAllocator = createExecutionSlotAllocator(inputLocationRetriever);

		inputLocationRetriever.markScheduled(producerId);
		inputLocationRetriever.markScheduled(consumerId);

		final List<ExecutionVertexSchedulingRequirements> schedulingRequirements = createSchedulingRequirements(producerId, consumerId);
		final Collection<SlotExecutionVertexAssignment> slotExecutionVertexAssignments = executionSlotAllocator.allocateSlotsFor(schedulingRequirements);

		final SlotExecutionVertexAssignment producerSlotAssignment = findSlotAssignmentByExecutionVertexId(producerId, slotExecutionVertexAssignments);
		final SlotExecutionVertexAssignment consumerSlotAssignment = findSlotAssignmentByExecutionVertexId(consumerId, slotExecutionVertexAssignments);

		assertTrue(producerSlotAssignment.getLogicalSlotFuture().isDone());
		assertFalse(consumerSlotAssignment.getLogicalSlotFuture().isDone());
	}

	@Test
	public void consumersAreAssignedToSlotsAfterProducers() {
		final ExecutionVertexID producerId = new ExecutionVertexID(new JobVertexID(), 0);
		final ExecutionVertexID consumerId = new ExecutionVertexID(new JobVertexID(), 0);

		final TestingInputLocationRetriever inputLocationRetriever = new TestingInputLocationRetriever.Builder()
			.connectConsumerToProducer(consumerId, producerId)
			.build();

		final DefaultExecutionSlotAllocator executionSlotAllocator = createExecutionSlotAllocator(inputLocationRetriever);

		inputLocationRetriever.markScheduled(producerId);
		inputLocationRetriever.markScheduled(consumerId);

		final List<ExecutionVertexSchedulingRequirements> schedulingRequirements = createSchedulingRequirements(producerId, consumerId);
		final Collection<SlotExecutionVertexAssignment> slotExecutionVertexAssignments = executionSlotAllocator.allocateSlotsFor(schedulingRequirements);
		inputLocationRetriever.assignTaskManagerLocation(producerId);

		final SlotExecutionVertexAssignment consumerSlotAssignment = findSlotAssignmentByExecutionVertexId(consumerId, slotExecutionVertexAssignments);
		assertTrue(consumerSlotAssignment.getLogicalSlotFuture().isDone());
	}

	private List<ExecutionVertexSchedulingRequirements> createSchedulingRequirements(final ExecutionVertexID producerId, final ExecutionVertexID consumerId) {
		final ExecutionVertexSchedulingRequirements consumerSchedulingRequirements = new ExecutionVertexSchedulingRequirements.Builder()
			.withExecutionVertexId(consumerId)
			.build();

		final ExecutionVertexSchedulingRequirements producerSchedulingRequirements = new ExecutionVertexSchedulingRequirements.Builder()
			.withExecutionVertexId(producerId)
			.build();

		return Arrays.asList(producerSchedulingRequirements, consumerSchedulingRequirements);
	}

	private DefaultExecutionSlotAllocator createExecutionSlotAllocator(final TestingInputLocationRetriever inputsLocationsRetriever) {
		return new DefaultExecutionSlotAllocator(
				new SimpleSlotProvider(new JobID(), 2),
				inputsLocationsRetriever,
				SLOT_ALLOCATION_TIMEOUT);
	}

	private SlotExecutionVertexAssignment findSlotAssignmentByExecutionVertexId(final ExecutionVertexID executionVertexId, final Collection<SlotExecutionVertexAssignment> assignments) {
		return assignments.stream()
			.filter(slotExecutionVertexAssignment -> slotExecutionVertexAssignment.getExecutionVertexId().equals(executionVertexId))
			.findFirst()
			.get();
	}

	static class TestingInputLocationRetriever implements InputsLocationsRetriever {

		private final Map<ExecutionVertexID, List<ExecutionVertexID>> producersByVertex;

		private final Map<ExecutionVertexID, CompletableFuture<TaskManagerLocation>> taskManagerLocationsByVertex = new HashMap<>();

		TestingInputLocationRetriever(final Map<ExecutionVertexID, List<ExecutionVertexID>> producersByVertex) {
			this.producersByVertex = new HashMap<>(producersByVertex);
		}

		@Override
		public Collection<Collection<ExecutionVertexID>> getConsumedResultPartitionsProducers(final ExecutionVertexID executionVertexId) {
			final Map<JobVertexID, List<ExecutionVertexID>> executionVerticesByJobVertex =
				producersByVertex.getOrDefault(executionVertexId, Collections.emptyList())
					.stream()
					.collect(Collectors.groupingBy(ExecutionVertexID::getJobVertexId));

			return new ArrayList<>(executionVerticesByJobVertex.values());
		}

		@Override
		public Optional<CompletableFuture<TaskManagerLocation>> getTaskManagerLocation(final ExecutionVertexID executionVertexId) {
			return Optional.ofNullable(taskManagerLocationsByVertex.get(executionVertexId));
		}

		public void markScheduled(final ExecutionVertexID executionVertexId) {
			taskManagerLocationsByVertex.put(executionVertexId, new CompletableFuture<>());
		}

		public void assignTaskManagerLocation(final ExecutionVertexID executionVertexId) {
			taskManagerLocationsByVertex.compute(executionVertexId, (key, future) -> {
				if (future == null) {
					return CompletableFuture.completedFuture(new LocalTaskManagerLocation());
				}
				future.complete(new LocalTaskManagerLocation());
				return future;
			});
		}

		static class Builder {

			private final Map<ExecutionVertexID, List<ExecutionVertexID>> consumerToProducers = new HashMap<>();

			public Builder connectConsumerToProducer(final ExecutionVertexID consumer, final ExecutionVertexID producer) {
				consumerToProducers.compute(consumer, (key, producers) -> {
					if (producers == null) {
						producers = new ArrayList<>();
					}
					producers.add(producer);
					return producers;
				});
				return this;
			}

			public TestingInputLocationRetriever build() {
				return new TestingInputLocationRetriever(consumerToProducers);
			}

		}
	}
}
