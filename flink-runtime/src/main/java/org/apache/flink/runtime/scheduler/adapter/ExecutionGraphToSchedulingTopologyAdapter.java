/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.InputsLocationsRetriever;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Adapter of {@link ExecutionGraph} to {@link SchedulingTopology}.
 */
public class ExecutionGraphToSchedulingTopologyAdapter implements SchedulingTopology, InputsLocationsRetriever {

	private final Map<ExecutionVertexID, DefaultSchedulingExecutionVertex> executionVerticesById;

	private final List<SchedulingExecutionVertex> executionVerticesList;

	private final Map<IntermediateResultPartitionID, ? extends SchedulingResultPartition> resultPartitionsById;

	private final Map<ExecutionVertexID, TaskManagerLocationSupplier> taskManagerLocationSuppliers = new HashMap<>();

	public ExecutionGraphToSchedulingTopologyAdapter(ExecutionGraph graph) {
		checkNotNull(graph, "execution graph can not be null");

		this.executionVerticesById = new HashMap<>();
		this.executionVerticesList = new ArrayList<>(graph.getTotalNumberOfVertices());
		Map<IntermediateResultPartitionID, DefaultSchedulingResultPartition> tmpResultPartitionsById = new HashMap<>();
		Map<ExecutionVertex, DefaultSchedulingExecutionVertex> executionVertexMap = new HashMap<>();

		for (ExecutionVertex vertex : graph.getAllExecutionVertices()) {

			List<DefaultSchedulingResultPartition> producedPartitions = generateProducedSchedulingResultPartition(vertex.getProducedPartitions());

			producedPartitions.forEach(partition -> tmpResultPartitionsById.put(partition.getId(), partition));

			DefaultSchedulingExecutionVertex schedulingVertex = generateSchedulingExecutionVertex(vertex, producedPartitions);
			this.executionVerticesById.put(schedulingVertex.getId(), schedulingVertex);
			this.executionVerticesList.add(schedulingVertex);
			executionVertexMap.put(vertex, schedulingVertex);

			taskManagerLocationSuppliers.put(schedulingVertex.getId(), new TaskManagerLocationSupplier(vertex));
		}
		this.resultPartitionsById = tmpResultPartitionsById;

		connectVerticesToConsumedPartitions(executionVertexMap, tmpResultPartitionsById);
	}

	@Override
	public Iterable<SchedulingExecutionVertex> getVertices() {
		return executionVerticesList;
	}

	@Override
	public Optional<SchedulingExecutionVertex> getVertex(ExecutionVertexID executionVertexId) {
		return Optional.ofNullable(executionVerticesById.get(executionVertexId));
	}

	@Override
	public Optional<SchedulingResultPartition> getResultPartition(IntermediateResultPartitionID intermediateResultPartitionId) {
		return Optional.ofNullable(resultPartitionsById.get(intermediateResultPartitionId));
	}

	private static List<DefaultSchedulingResultPartition> generateProducedSchedulingResultPartition(
		Map<IntermediateResultPartitionID, IntermediateResultPartition> producedIntermediatePartitions) {

		List<DefaultSchedulingResultPartition> producedSchedulingPartitions = new ArrayList<>(producedIntermediatePartitions.size());

		producedIntermediatePartitions.values().forEach(
			irp -> producedSchedulingPartitions.add(
				new DefaultSchedulingResultPartition(
					irp.getPartitionId(),
					irp.getIntermediateResult().getId(),
					irp.getResultType())));

		return producedSchedulingPartitions;
	}

	private static DefaultSchedulingExecutionVertex generateSchedulingExecutionVertex(
		ExecutionVertex vertex,
		List<DefaultSchedulingResultPartition> producedPartitions) {

		DefaultSchedulingExecutionVertex schedulingVertex = new DefaultSchedulingExecutionVertex(
			new ExecutionVertexID(vertex.getJobvertexId(), vertex.getParallelSubtaskIndex()),
			producedPartitions,
			new ExecutionStateSupplier(vertex),
			vertex.getInputDependencyConstraint());

		producedPartitions.forEach(partition -> partition.setProducer(schedulingVertex));

		return schedulingVertex;
	}

	private static void connectVerticesToConsumedPartitions(
		Map<ExecutionVertex, DefaultSchedulingExecutionVertex> executionVertexMap,
		Map<IntermediateResultPartitionID, DefaultSchedulingResultPartition> resultPartitions) {

		for (Map.Entry<ExecutionVertex, DefaultSchedulingExecutionVertex> mapEntry : executionVertexMap.entrySet()) {
			final DefaultSchedulingExecutionVertex schedulingVertex = mapEntry.getValue();
			final ExecutionVertex executionVertex = mapEntry.getKey();

			for (int index = 0; index < executionVertex.getNumberOfInputs(); index++) {
				for (ExecutionEdge edge : executionVertex.getInputEdges(index)) {
					DefaultSchedulingResultPartition partition = resultPartitions.get(edge.getSource().getPartitionId());
					schedulingVertex.addConsumedPartition(partition);
					partition.addConsumer(schedulingVertex);
				}
			}
		}
	}

	@Override
	public Collection<Collection<ExecutionVertexID>> getConsumedResultPartitionsProducers(final ExecutionVertexID executionVertexId) {
		final Optional<SchedulingExecutionVertex> vertex = getVertex(executionVertexId);
		final SchedulingExecutionVertex schedulingExecutionVertex = vertex.get();
		final Map<JobVertexID, List<ExecutionVertexID>> producersByJobVertex = schedulingExecutionVertex
			.getConsumedResultPartitions()
			.stream()
			.map(SchedulingResultPartition::getProducer)
			.map(SchedulingExecutionVertex::getId)
			.collect(Collectors.groupingBy(ExecutionVertexID::getJobVertexId));

		return new ArrayList<>(producersByJobVertex.values());
	}

	@Override
	public Optional<CompletableFuture<TaskManagerLocation>> getTaskManagerLocation(final ExecutionVertexID executionVertexId) {
		final Optional<SchedulingExecutionVertex> vertex = getVertex(executionVertexId);
		final SchedulingExecutionVertex schedulingExecutionVertex = vertex.get();
		if (schedulingExecutionVertex.getState() == ExecutionState.SCHEDULED) {
			return Optional.of(taskManagerLocationSuppliers.get(executionVertexId).get());
		} else {
			return Optional.empty();
		}
	}

	private static class ExecutionStateSupplier implements Supplier<ExecutionState> {

		private final ExecutionVertex executionVertex;

		ExecutionStateSupplier(ExecutionVertex vertex) {
			executionVertex = checkNotNull(vertex);
		}

		@Override
		public ExecutionState get() {
			return executionVertex.getExecutionState();
		}
	}

	private static class TaskManagerLocationSupplier implements Supplier<CompletableFuture<TaskManagerLocation>> {

		private final ExecutionVertex executionVertex;

		TaskManagerLocationSupplier(ExecutionVertex vertex) {
			executionVertex = checkNotNull(vertex);
		}

		@Override
		public CompletableFuture<TaskManagerLocation> get() {
			return executionVertex.getCurrentTaskManagerLocationFuture();
		}
	}
}
