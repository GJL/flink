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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.failover.flip1.ExecutionFailureHandler;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailureHandlingResult;
import org.apache.flink.runtime.executiongraph.restart.ThrowingRestartStrategy;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTracker;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyFactory;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Stub implementation of the future default scheduler.
 */
public class DefaultScheduler extends LegacyScheduler implements SchedulerOperations {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultScheduler.class);

	private final ClassLoader userCodeLoader;

	private final SchedulingStrategyFactory schedulingStrategyFactory;

	private final ExecutionSlotAllocator executionSlotAllocator;

	private final ExecutionFailureHandler executionFailureHandler;

	private final ScheduledExecutorService futureExecutor;

	private Comparator<SlotExecutionVertexAssignment> bla;

	private SchedulingStrategy schedulingStrategy;

	public DefaultScheduler(
			final Logger log,
			final JobGraph jobGraph,
			final BackPressureStatsTracker backPressureStatsTracker,
			final Executor ioExecutor,
			final Configuration jobMasterConfiguration,
			final SlotProvider slotProvider,
			final ScheduledExecutorService futureExecutor,
			final ClassLoader userCodeLoader,
			final CheckpointRecoveryFactory checkpointRecoveryFactory,
			final Time rpcTimeout,
			final BlobWriter blobWriter,
			final JobManagerJobMetricGroup jobManagerJobMetricGroup,
			final Time slotRequestTimeout,
			final SchedulingStrategyFactory schedulingStrategyFactory,
			final ExecutionSlotAllocator executionSlotAllocator,
			final ExecutionFailureHandler executionFailureHandler) throws Exception {

		super(
			log,
			jobGraph,
			backPressureStatsTracker,
			ioExecutor,
			jobMasterConfiguration,
			slotProvider,
			futureExecutor,
			userCodeLoader,
			checkpointRecoveryFactory,
			rpcTimeout,
			new ThrowingRestartStrategy.ThrowingRestartStrategyFactory(),
			blobWriter,
			jobManagerJobMetricGroup,
			slotRequestTimeout);

		this.futureExecutor = futureExecutor;
		this.userCodeLoader = userCodeLoader;
		this.schedulingStrategyFactory = schedulingStrategyFactory;
		this.executionSlotAllocator = executionSlotAllocator;
		this.executionFailureHandler = executionFailureHandler;
		this.bla = Comparator.comparing(
			SlotExecutionVertexAssignment::getExecutionVertexId,
			new ExecutionVertexIDTopologicalComparator(jobGraph));
	}

	// ------------------------------------------------------------------------
	// SchedulerNG
	// ------------------------------------------------------------------------

	@Override
	public void startScheduling() {
		initializeScheduling();
		schedulingStrategy.startScheduling();
	}

	private void initializeScheduling() {
		schedulingStrategy = schedulingStrategyFactory.createInstance(this, getSchedulingTopology(), getJobGraph());
		scheduleForExecution();
	}

	@Override
	public boolean updateTaskExecutionState(final TaskExecutionState taskExecutionState) {
		final Optional<ExecutionVertexID> executionVertexIdOptional = getExecutionVertexId(taskExecutionState.getID());
		if (executionVertexIdOptional.isPresent()) {
			final ExecutionVertexID executionVertexId = executionVertexIdOptional.get();
			updateState(taskExecutionState);
			schedulingStrategy.onExecutionStateChange(executionVertexId, taskExecutionState.getExecutionState());
			maybeHandleTaskFailure(taskExecutionState, executionVertexId);
			return true;
		}

		return false;
	}

	private void maybeHandleTaskFailure(final TaskExecutionState taskExecutionState, final ExecutionVertexID executionVertexId) {
		if (taskExecutionState.getExecutionState() == ExecutionState.FAILED) {
			final FailureHandlingResult failureHandlingResult = executionFailureHandler.getFailureHandlingResult(
				executionVertexId,
				taskExecutionState.getError(userCodeLoader));

			maybeRestartTasks(failureHandlingResult);
		}
	}

	private void maybeRestartTasks(final FailureHandlingResult failureHandlingResult) {
		if (failureHandlingResult.canRestart()) {
			restartTasksWithDelay(failureHandlingResult);
		}
	}

	private void restartTasksWithDelay(final FailureHandlingResult failureHandlingResult) {
		final Set<ExecutionVertexID> verticesToRestart = failureHandlingResult.getVerticesToRestart();
		final CompletableFuture<?> cancelFuture = cancelTasksAsync(verticesToRestart);
		futureExecutor.schedule(
			() -> cancelFuture.whenComplete(restartTasksOrHandleError(verticesToRestart)),
			failureHandlingResult.getRestartDelayMS(),
			TimeUnit.MILLISECONDS);
	}

	private BiConsumer<Object, Throwable> restartTasksOrHandleError(final Set<ExecutionVertexID> verticesToRestart) {
		return (Object ignored, Throwable throwable) -> {
			if (throwable == null) {
				// TODO: check global mod version
				schedulingStrategy.restartTasks(verticesToRestart);
			} else {
				// TODO: fatal error
			}
		};
	}

	private CompletableFuture<?> cancelTasksAsync(final Set<ExecutionVertexID> verticesToRestart) {
		final List<CompletableFuture<?>> cancelFutures = verticesToRestart.stream()
			.map(executionVertexID -> getExecutionVertex(executionVertexID).cancel())
			.collect(Collectors.toList());

		return FutureUtils.combineAll(cancelFutures);
	}

	@Override
	public void scheduleOrUpdateConsumers(final ResultPartitionID partitionID) {
		// TODO: implement, for now: noop

		LOG.info("scheduleOrUpdateConsumers");
	}

	// ------------------------------------------------------------------------
	// SchedulerOperations
	// ------------------------------------------------------------------------

	@Override
	public void allocateSlotsAndDeploy(final Collection<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		final List<SlotExecutionVertexAssignment> slotExecutionVertexAssignments = new ArrayList<>(allocateSlots(executionVertexDeploymentOptions));
		slotExecutionVertexAssignments.sort(bla);

		CompletableFuture<?> predecessorFuture = CompletableFuture.completedFuture(null);
		for (SlotExecutionVertexAssignment slotExecutionVertexAssignment : slotExecutionVertexAssignments) {
			final CompletableFuture<LogicalSlot> logicalSlotCompletableFuture = slotExecutionVertexAssignment.getLogicalSlotFuture()
				.thenCombine(predecessorFuture, (logicalSlot, ignored) -> logicalSlot)
				.whenComplete(deployTaskOrHandleError(slotExecutionVertexAssignment.getExecutionVertexId()));
			predecessorFuture = logicalSlotCompletableFuture;
		}
	}

	private Collection<SlotExecutionVertexAssignment> allocateSlots(final Collection<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		return executionSlotAllocator.allocateSlotsFor(
				executionVertexDeploymentOptions.stream()
					.map(ExecutionVertexDeploymentOption::getExecutionVertexId)
					.map(this::createSchedulingRequirements)
					.collect(Collectors.toList()));
	}

	private BiConsumer<LogicalSlot, Throwable> deployTaskOrHandleError(final ExecutionVertexID executionVertexId) {
		return (logicalSlot, throwable) -> {
			if (throwable == null) {
				final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);
				try {
					// TODO: check if version matches, i.e., deployment should be conducted, maybe do before calling this
					// TODO: reset for new execution
					executionVertex.deployToSlot(logicalSlot);
				} catch (JobException e) {
					e.printStackTrace();
				}
			} else {
				// TODO: handle failure
				throwable.printStackTrace();
			}
		};
	}

	private ExecutionVertexSchedulingRequirements createSchedulingRequirements(final ExecutionVertexID executionVertexId) {
		final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);
		final AllocationID latestPriorAllocation = executionVertex.getLatestPriorAllocation();

		return new ExecutionVertexSchedulingRequirements(
			executionVertexId,
			latestPriorAllocation,
			ResourceProfile.UNKNOWN,
			executionVertex.getJobVertex().getSlotSharingGroup().getSlotSharingGroupId(),
			executionVertex.getLocationConstraint(),
			null,
			Collections.emptyList());
	}
}
