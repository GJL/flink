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
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.UpdateTaskExecutionStateInDefaultSchedulerListener;
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
import org.apache.flink.runtime.scheduler.strategy.LazyFromSourcesSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyFactory;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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

	private final ScheduledExecutor delayExecutor;

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
		this.delayExecutor = delayExecutor;
		this.userCodeLoader = userCodeLoader;
		this.schedulingStrategyFactory = checkNotNull(schedulingStrategyFactory);
		this.failoverStrategyFactory = checkNotNull(failoverStrategyFactory);
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
		executionFailureHandler = new ExecutionFailureHandler(failoverStrategyFactory.create(getFailoverTopology()), restartBackoffTimeStrategy);
		schedulingStrategy = schedulingStrategyFactory.createInstance(this, getSchedulingTopology(), getJobGraph());
		executionSlotAllocator = new DefaultExecutionSlotAllocator(slotProvider, getSchedulingTopology(), slotRequestTimeout);
		setTaskFailureListener(new UpdateTaskExecutionStateInDefaultSchedulerListener(this, getJobGraph().getJobID()));
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

		final Set<ExecutionVertexVersion> executionVertexVersions =
			new HashSet<>(executionVertexVersioner.recordVertexModifications(verticesToRestart).values());

		final CompletableFuture<?> cancelFuture = cancelTasksAsync(verticesToRestart);

		delayExecutor.schedule(
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
		final Map<ExecutionVertexID, ExecutionVertexDeploymentOption> deploymentOptionsByVertex = groupDeploymentOptionsByVertexId(executionVertexDeploymentOptions);
		final Set<ExecutionVertexID> verticesToDeploy = deploymentOptionsByVertex.keySet();
		final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex = executionVertexVersioner.recordVertexModifications(verticesToDeploy);

		prepareToDeployVertices(verticesToDeploy);

		final Collection<SlotExecutionVertexAssignment> slotExecutionVertexAssignments = allocateSlots(executionVertexDeploymentOptions);

		final Collection<DeploymentHandle> deploymentHandles = createDeploymentHandles(
			requiredVersionByVertex,
			deploymentOptionsByVertex,
			slotExecutionVertexAssignments);

		if (isDeployIndividually()) {
			deployIndividually(deploymentHandles);
		} else {
			waitForAllSlotsAndDeploy(deploymentHandles);
		}
	}

	private static Map<ExecutionVertexID, ExecutionVertexDeploymentOption> groupDeploymentOptionsByVertexId(
			final Collection<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		return executionVertexDeploymentOptions.stream().collect(Collectors.toMap(
				ExecutionVertexDeploymentOption::getExecutionVertexId,
				Function.identity()));
	}

	private void prepareToDeployVertices(final Set<ExecutionVertexID> verticesToDeploy) {
		cancelSlotAssignments(verticesToDeploy);
		resetForNewExecution(verticesToDeploy);
		transitionToScheduled(verticesToDeploy);
	}

	private void cancelSlotAssignments(final Collection<ExecutionVertexID> vertices) {
		vertices.forEach(executionVertexId -> executionSlotAllocator.cancel(executionVertexId));
	}

	private Collection<SlotExecutionVertexAssignment> allocateSlots(final Collection<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		return executionSlotAllocator.allocateSlotsFor(executionVertexDeploymentOptions
			.stream()
			.map(ExecutionVertexDeploymentOption::getExecutionVertexId)
			.map(this::createSchedulingRequirements)
			.collect(Collectors.toList()));
	}

	private ExecutionVertexSchedulingRequirements createSchedulingRequirements(final ExecutionVertexID executionVertexId) {
		final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);
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

	private static Collection<DeploymentHandle> createDeploymentHandles(
		final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex,
		final Map<ExecutionVertexID, ExecutionVertexDeploymentOption> deploymentOptionsByVertex,
		final Collection<SlotExecutionVertexAssignment> slotExecutionVertexAssignments) {

		return slotExecutionVertexAssignments
			.stream()
			.map(slotExecutionVertexAssignment -> {
				final ExecutionVertexID executionVertexId = slotExecutionVertexAssignment.getExecutionVertexId();
				return new DeploymentHandle(
					requiredVersionByVertex.get(executionVertexId),
					deploymentOptionsByVertex.get(executionVertexId),
					slotExecutionVertexAssignment);
			})
			.collect(Collectors.toList());
	}

	private boolean isDeployIndividually() {
		return schedulingStrategy instanceof LazyFromSourcesSchedulingStrategy;
	}

	private void deployIndividually(final Collection<DeploymentHandle> deploymentHandles) {
		for (final DeploymentHandle deploymentHandle : deploymentHandles) {
			final ExecutionVertexVersion requiredVertexVersion = deploymentHandle.getExecutionVertexVersion();
			FutureUtils.assertNoException(
				deploymentHandle
					.getSlotExecutionVertexAssignment()
					.getLogicalSlotFuture()
					.handle(assignResourceOrHandleError(requiredVertexVersion))
					.handle(deployOrHandleError(requiredVertexVersion, deploymentHandle.getDeploymentOption())));
		}
	}

	private void waitForAllSlotsAndDeploy(final Collection<DeploymentHandle> deploymentHandles) {
		FutureUtils.assertNoException(
			assignAllResources(deploymentHandles).handle(deployAll(deploymentHandles)));
	}

	private FutureUtils.ConjunctFuture<Void> assignAllResources(final Collection<DeploymentHandle> deploymentHandles) {
		final List<CompletableFuture<Void>> slotAssignedFutures = new ArrayList<>();
		for (DeploymentHandle deploymentHandle : deploymentHandles) {
			final CompletableFuture<Void> slotAssigned = deploymentHandle
				.getSlotExecutionVertexAssignment()
				.getLogicalSlotFuture()
				.handle(assignResourceOrHandleError(deploymentHandle.getExecutionVertexVersion()));
			slotAssignedFutures.add(slotAssigned);
		}
		return FutureUtils.waitForAll(slotAssignedFutures);
	}

	private BiFunction<Void, Throwable, Void> deployAll(final Collection<DeploymentHandle> deploymentHandles) {
		return (aVoid, ignored) -> {
			for (final DeploymentHandle deploymentHandle : deploymentHandles) {
				final SlotExecutionVertexAssignment slotExecutionVertexAssignment = deploymentHandle.getSlotExecutionVertexAssignment();
				final CompletableFuture<LogicalSlot> slotAssigned = slotExecutionVertexAssignment.getLogicalSlotFuture();
				checkState(slotAssigned.isDone());

				FutureUtils.assertNoException(
					slotAssigned.handle(deployOrHandleError(
						deploymentHandle.getExecutionVertexVersion(),
						deploymentHandle.getDeploymentOption())));
			}
			return null;
		};
	}

	private BiFunction<LogicalSlot, Throwable, Void> assignResourceOrHandleError(final ExecutionVertexVersion executionVertexVersion) {
		return (logicalSlot, throwable) -> {

			if (executionVertexVersioner.isModified(executionVertexVersion)) {
				return null;
			}

			final ExecutionVertexID executionVertexId = executionVertexVersion.getExecutionVertexId();
			if (throwable == null) {
				final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);
				// TODO: why not in tryAssignResource ?
				executionVertex.getCurrentExecutionAttempt().registerProducedPartitions(logicalSlot.getTaskManagerLocation());
				executionVertex.tryAssignResource(logicalSlot);
			} else {
				handleTaskFailure(executionVertexId, throwable);
			}

			return null;
		};
	}

	private BiFunction<Object, Throwable, Void> deployOrHandleError(
			final ExecutionVertexVersion requiredVertexVersion,
			final ExecutionVertexDeploymentOption executionVertexDeploymentOption) {

		return (ignored, throwable) -> {

			if (executionVertexVersioner.isModified(requiredVertexVersion)) {
				return null;
			}

			if (throwable == null) {
				deployTaskSafe(executionVertexDeploymentOption);
			} else {
				final ExecutionVertexID executionVertexId = requiredVertexVersion.getExecutionVertexId();
				handleTaskFailure(executionVertexId, throwable);
			}

			return null;
		};
	}

	private void deployTaskSafe(final ExecutionVertexDeploymentOption executionVertexDeploymentOption) {
		try {
			deployTask(executionVertexDeploymentOption);
		} catch (Throwable e) {
			handleTaskFailure(executionVertexDeploymentOption.getExecutionVertexId(), e);
		}
	}

	private void deployTask(final ExecutionVertexDeploymentOption executionVertexDeploymentOption) throws JobException {
		final ExecutionVertexID executionVertexId = executionVertexDeploymentOption.getExecutionVertexId();
		final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);
		final DeploymentOption deploymentOption = executionVertexDeploymentOption.getDeploymentOption();

		executionVertex.setSendScheduleOrUpdateConsumerMessage(deploymentOption.sendScheduleOrUpdateConsumerMessage());
		executionVertex.deploy();
	}
}
