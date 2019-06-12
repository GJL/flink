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
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.failover.flip1.ExecutionFailureHandler;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailureHandlingResult;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.restart.ThrowingRestartStrategy;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Stub implementation of the future default scheduler.
 */
public class DefaultScheduler extends LegacyScheduler implements SchedulerOperations {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultScheduler.class);

	private final ClassLoader userCodeLoader;

	private final SchedulingStrategyFactory schedulingStrategyFactory;

	private final SlotProvider slotProvider;

	private final Time slotRequestTimeout;

	private final RestartBackoffTimeStrategy restartBackoffTimeStrategy;

	private ExecutionSlotAllocator executionSlotAllocator;

	private ExecutionFailureHandler executionFailureHandler;

	private final FailoverStrategy.Factory failoverStrategyFactory;

	private final ScheduledExecutor futureExecutor;

	private Comparator<SlotExecutionVertexAssignment> slotExecutionVertexAssignmentComparator;

	private SchedulingStrategy schedulingStrategy;

	private final ExecutionVertexVersioner executionVertexVersioner = new ExecutionVertexVersioner();

	public DefaultScheduler(
		final Logger log,
		final JobGraph jobGraph,
		final BackPressureStatsTracker backPressureStatsTracker,
		final Executor ioExecutor,
		final Configuration jobMasterConfiguration,
		final SlotProvider slotProvider,
		final ScheduledExecutorService futureExecutor,
		final ScheduledExecutor delayExecutor,
		final ClassLoader userCodeLoader,
		final CheckpointRecoveryFactory checkpointRecoveryFactory,
		final Time rpcTimeout,
		final BlobWriter blobWriter,
		final JobManagerJobMetricGroup jobManagerJobMetricGroup,
		final Time slotRequestTimeout,
		final SchedulingStrategyFactory schedulingStrategyFactory,
		final FailoverStrategy.Factory failoverStrategyFactory,
		final RestartBackoffTimeStrategy restartBackoffTimeStrategy) throws Exception {

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

		this.restartBackoffTimeStrategy = restartBackoffTimeStrategy;
		this.slotRequestTimeout = slotRequestTimeout;
		this.slotProvider = slotProvider;
		// TODO: darf man benutzen?
		this.futureExecutor = delayExecutor;
		this.userCodeLoader = userCodeLoader;
		this.schedulingStrategyFactory = checkNotNull(schedulingStrategyFactory);
		this.failoverStrategyFactory = checkNotNull(failoverStrategyFactory);
		this.slotExecutionVertexAssignmentComparator = Comparator.comparing(
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

	@Override
	public void suspend(final Throwable cause) {
		super.suspend(cause);
	}

	private void initializeScheduling() {
		executionFailureHandler = new ExecutionFailureHandler(failoverStrategyFactory.create(getFailoverTopology()), restartBackoffTimeStrategy);
		schedulingStrategy = schedulingStrategyFactory.createInstance(this, getSchedulingTopology(), getJobGraph());
		executionSlotAllocator = new DefaultExecutionSlotAllocator(slotProvider, getSchedulingTopology(), slotRequestTimeout);
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
			final Throwable error = taskExecutionState.getError(userCodeLoader);
			handleTaskFailure(executionVertexId, error);
		}
	}

	private void handleTaskFailure(final ExecutionVertexID executionVertexId, final Throwable error) {
		final FailureHandlingResult failureHandlingResult = executionFailureHandler.getFailureHandlingResult(executionVertexId, error);
		maybeRestartTasks(failureHandlingResult);
	}

	private void maybeRestartTasks(final FailureHandlingResult failureHandlingResult) {
		if (failureHandlingResult.canRestart()) {
			restartTasksWithDelay(failureHandlingResult);
		} else {
			failJob(failureHandlingResult.getError());
		}
	}

	private void restartTasksWithDelay(final FailureHandlingResult failureHandlingResult) {
		final Set<ExecutionVertexID> verticesToRestart = failureHandlingResult.getVerticesToRestart();
		final Set<ExecutionVertexVersion> executionVertexVersions = verticesToRestart.stream()
			.map(executionVertexVersioner::recordModification)
			.collect(Collectors.toSet());

		final CompletableFuture<?> cancelFuture = cancelTasksAsync(verticesToRestart);

		futureExecutor.schedule(
			() -> FutureUtils.assertNoException(
				cancelFuture.handleAsync(restartTasksOrHandleError(executionVertexVersions), getMainThreadExecutor())),
			failureHandlingResult.getRestartDelayMS(),
			TimeUnit.MILLISECONDS);
	}

	private BiFunction<Object, Throwable, Void> restartTasksOrHandleError(final Set<ExecutionVertexVersion> executionVertexVersions) {
		return (Object ignored, Throwable throwable) -> {

			if (throwable == null) {
				final Set<ExecutionVertexID> verticesToRestart = executionVertexVersioner.getUnmodifiedExecutionVertices(executionVertexVersions);
				schedulingStrategy.restartTasks(verticesToRestart);
			} else {
				failJob(throwable);
			}
			return null;
		};
	}

	private CompletableFuture<?> cancelTasksAsync(final Set<ExecutionVertexID> verticesToRestart) {
		final List<CompletableFuture<?>> cancelFutures = verticesToRestart.stream()
			.map(this::cancelExecutionVertex)
			.collect(Collectors.toList());

		return FutureUtils.combineAll(cancelFutures);
	}

	private CompletableFuture<?> cancelExecutionVertex(final ExecutionVertexID executionVertexId) {
		return getExecutionVertex(executionVertexId).cancel();
	}

	@Override
	public void scheduleOrUpdateConsumers(final ResultPartitionID partitionId) {
		final Optional<ExecutionVertexID> producerVertexId = getExecutionVertexId(partitionId.getProducerId());
		if (producerVertexId.isPresent()) {
			egScheduleOrUpdateConsumers(partitionId);
			schedulingStrategy.onPartitionConsumable(producerVertexId.get(), partitionId);
		}
	}

	// ------------------------------------------------------------------------
	// SchedulerOperations
	// ------------------------------------------------------------------------

	@Override
	public void allocateSlotsAndDeploy(final Collection<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		transitionToScheduled(executionVertexDeploymentOptions);

		final Map<ExecutionVertexID, ExecutionVertexDeploymentOption> deploymentOptionsByVertex = getExecutionVertexIDExecutionVertexDeploymentOptionMap(executionVertexDeploymentOptions);

		final Collection<SlotExecutionVertexAssignment> slotExecutionVertexAssignments = allocateSlots(executionVertexDeploymentOptions);

		final Map<ExecutionVertexID, ExecutionVertexVersion> collect = executionVertexDeploymentOptions.stream()
			.map(ExecutionVertexDeploymentOption::getExecutionVertexId)
			.map(executionVertexVersioner::recordModification)
			.collect(Collectors.toMap(ExecutionVertexVersion::getExecutionVertexId, Function.identity()));

		CompletableFuture<Void> bla = FutureUtils.waitForAll(slotExecutionVertexAssignments.stream()
			.map(slotExecutionVertexAssignment -> {
				return slotExecutionVertexAssignment.getLogicalSlotFuture().thenAccept(logicalSlot -> {
					getExecutionVertex(slotExecutionVertexAssignment.getExecutionVertexId()).tryAssignResource(logicalSlot);
				});
			})
			.collect(Collectors.toList()));

		FutureUtils.assertNoException(bla.handle((ignored, throwable) -> {
			if (throwable == null) {
				final ArrayList<SlotExecutionVertexAssignment> slotExecutionVertexAssignments1 = new ArrayList<>(slotExecutionVertexAssignments);
				slotExecutionVertexAssignments1.sort(slotExecutionVertexAssignmentComparator);

				for (SlotExecutionVertexAssignment slotExecutionVertexAssignment : slotExecutionVertexAssignments1) {

					final ExecutionVertexVersion executionVertexVersion = collect.get(slotExecutionVertexAssignment.getExecutionVertexId());

					checkState(slotExecutionVertexAssignment.getLogicalSlotFuture().isDone());

					final CompletableFuture<LogicalSlot> logicalSlotCompletableFuture = slotExecutionVertexAssignment.getLogicalSlotFuture()
						.whenComplete(deployTaskOrHandleError(executionVertexVersion, deploymentOptionsByVertex.get(executionVertexVersion.getExecutionVertexId())));

					logicalSlotCompletableFuture.exceptionally(throwable1 -> {
							// TODO: cancel assignments
							handleTaskFailure(executionVertexVersion.getExecutionVertexId(), throwable1);
							return null;
						}
					);
				}
			} else {
				// TODO: cancel assignments
			}
			return null;
		}));
	}

	private Map<ExecutionVertexID, ExecutionVertexDeploymentOption> getExecutionVertexIDExecutionVertexDeploymentOptionMap(final Collection<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		return executionVertexDeploymentOptions.stream()
				.collect(Collectors.toMap(
					ExecutionVertexDeploymentOption::getExecutionVertexId,
					Function.identity()));
	}

	private void transitionToScheduled(final Collection<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		executionVertexDeploymentOptions.forEach(executionVertexDeploymentOption -> getExecutionVertex(executionVertexDeploymentOption.getExecutionVertexId()).getCurrentExecutionAttempt().transitionState(ExecutionState.SCHEDULED));
	}

	private Collection<SlotExecutionVertexAssignment> allocateSlots(final Collection<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		return executionSlotAllocator.allocateSlotsFor(
				executionVertexDeploymentOptions.stream()
					.map(ExecutionVertexDeploymentOption::getExecutionVertexId)
					.map(this::createSchedulingRequirements)
					.collect(Collectors.toList()));
	}

	private ExecutionVertexSchedulingRequirements createSchedulingRequirements(final ExecutionVertexID executionVertexId) {
		final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);
		final AllocationID latestPriorAllocation = executionVertex.getLatestPriorAllocation();
		final SlotSharingGroup slotSharingGroup = executionVertex.getJobVertex().getSlotSharingGroup();

		return new ExecutionVertexSchedulingRequirements(
			executionVertexId,
			latestPriorAllocation,
			ResourceProfile.UNKNOWN,
			slotSharingGroup == null ? null : slotSharingGroup.getSlotSharingGroupId(),
			executionVertex.getLocationConstraint(),
			Collections.emptyList());
	}

	private BiConsumer<LogicalSlot, Throwable> deployTaskOrHandleError(
			final ExecutionVertexVersion executionVertexVersion,
			final ExecutionVertexDeploymentOption executionVertexDeploymentOption) {
		return (ignored, throwable) -> {
			if (executionVertexVersioner.isModified(executionVertexVersion)) {
				return;
			}

			final ExecutionVertexID executionVertexId = executionVertexVersion.getExecutionVertexId();
			if (throwable == null) {
				deployToSlot(executionVertexDeploymentOption);
			} else {
				handleTaskFailure(executionVertexId, throwable);
			}
		};
	}

	private void deployToSlot(final ExecutionVertexDeploymentOption executionVertexDeploymentOption) {
		final ExecutionVertexID executionVertexId = executionVertexDeploymentOption.getExecutionVertexId();
		final DeploymentOption deploymentOption = executionVertexDeploymentOption.getDeploymentOption();
		final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);
		try {
			executionVertex.setSendScheduleOrUpdateConsumerMessage(deploymentOption.sendScheduleOrUpdateConsumerMessage());
			executionVertex.maybeResetForNewExecution();
			executionVertex.deploy();
		} catch (JobException e) {
			handleTaskFailure(executionVertexId, e);
		}
	}
}
