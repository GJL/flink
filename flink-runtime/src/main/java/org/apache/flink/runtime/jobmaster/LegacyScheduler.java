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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphException;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.KvStateLocationRegistry;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTracker;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStats;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class LegacyScheduler implements SchedulerNG {

	private final JobGraph jobGraph;

	private final ExecutionGraph executionGraph;

	private final Logger log;

	private final BackPressureStatsTracker backPressureStatsTracker;

	private ComponentMainThreadExecutor mainThreadExecutor;

	private final Executor ioExecutor;

	public LegacyScheduler(final JobGraph jobGraph, final ExecutionGraph executionGraph, final Logger log, final BackPressureStatsTracker backPressureStatsTracker, final Executor ioExecutor) {
		this.jobGraph = checkNotNull(jobGraph);
		this.executionGraph = checkNotNull(executionGraph);
		this.log = checkNotNull(log);
		this.backPressureStatsTracker = checkNotNull(backPressureStatsTracker);
		this.ioExecutor = checkNotNull(ioExecutor);
	}

	@Override
	public void startScheduling() {
		try {
			executionGraph.scheduleForExecution();
		}
		catch (Throwable t) {
			executionGraph.failGlobal(t);
		}
	}

	@Override
	public void suspend(Throwable cause) {
		executionGraph.suspend(cause);
	}

	@Override
	public void cancel() {
		executionGraph.cancel();
	}

	@Override
	public boolean updateTaskExecutionState(final TaskExecutionState taskExecutionState) {
		return executionGraph.updateState(taskExecutionState);
	}

	@Override
	public SerializedInputSplit requestNextInputSplit(JobVertexID vertexID, ExecutionAttemptID executionAttempt) throws IOException {

		final Execution execution = executionGraph.getRegisteredExecutions().get(executionAttempt);
		if (execution == null) {
			// can happen when JobManager had already unregistered this execution upon on task failure,
			// but TaskManager get some delay to aware of that situation
			if (log.isDebugEnabled()) {
				log.debug("Can not find Execution for attempt {}.", executionAttempt);
			}
			// but we should TaskManager be aware of this
			throw new IllegalArgumentException("Can not find Execution for attempt " + executionAttempt);
		}

		final ExecutionJobVertex vertex = executionGraph.getJobVertex(vertexID);
		if (vertex == null) {
			log.error("Cannot find execution vertex for vertex ID {}.", vertexID);
			throw new IllegalArgumentException("Cannot find execution vertex for vertex ID " + vertexID);
		}

		if (vertex.getSplitAssigner() == null) {
			log.error("No InputSplitAssigner for vertex ID {}.", vertexID);
			throw new IllegalStateException("No InputSplitAssigner for vertex ID " + vertexID);
		}

		final InputSplit nextInputSplit = execution.getNextInputSplit();

		if (log.isDebugEnabled()) {
			log.debug("Send next input split {}.", nextInputSplit);
		}

		try {
			final byte[] serializedInputSplit = InstantiationUtil.serializeObject(nextInputSplit);
			return new SerializedInputSplit(serializedInputSplit);
		} catch (Exception ex) {
			log.error("Could not serialize the next input split of class {}.", nextInputSplit.getClass(), ex);
			IOException reason = new IOException("Could not serialize the next input split of class " +
				nextInputSplit.getClass() + ".", ex);
			vertex.fail(reason);
			throw reason;
		}
	}

	@Override
	public ExecutionState requestPartitionState(
			final IntermediateDataSetID intermediateResultId,
			final ResultPartitionID resultPartitionId) throws PartitionProducerDisposedException {

		final Execution execution = executionGraph.getRegisteredExecutions().get(resultPartitionId.getProducerId());
		if (execution != null) {
			return execution.getState();
		}
		else {
			final IntermediateResult intermediateResult =
				executionGraph.getAllIntermediateResults().get(intermediateResultId);

			if (intermediateResult != null) {
				// Try to find the producing execution
				Execution producerExecution = intermediateResult
					.getPartitionById(resultPartitionId.getPartitionId())
					.getProducer()
					.getCurrentExecutionAttempt();

				if (producerExecution.getAttemptId().equals(resultPartitionId.getProducerId())) {
					return producerExecution.getState();
				} else {
					throw new PartitionProducerDisposedException(resultPartitionId);
				}
			} else {
				throw new IllegalArgumentException("Intermediate data set with ID "
					+ intermediateResultId + " not found.");
			}
		}
	}

	@Override
	public void scheduleOrUpdateConsumers(final ResultPartitionID partitionID) {
		try {
			executionGraph.scheduleOrUpdateConsumers(partitionID);
		} catch (ExecutionGraphException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public KvStateLocation requestKvStateLocation(final JobID jobId, final String registrationName) throws UnknownKvStateLocation, FlinkJobNotFoundException {
		// sanity check for the correct JobID
		if (jobGraph.getJobID().equals(jobId)) {
			if (log.isDebugEnabled()) {
				log.debug("Lookup key-value state for job {} with registration " +
					"name {}.", jobGraph.getJobID(), registrationName);
			}

			final KvStateLocationRegistry registry = executionGraph.getKvStateLocationRegistry();
			final KvStateLocation location = registry.getKvStateLocation(registrationName);
			if (location != null) {
				return location;
			} else {
				throw new UnknownKvStateLocation(registrationName);
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Request of key-value state location for unknown job {} received.", jobId);
			}
			throw new FlinkJobNotFoundException(jobId);
		}
	}

	@Override
	public void notifyKvStateRegistered(final JobID jobId, final JobVertexID jobVertexId, final KeyGroupRange keyGroupRange, final String registrationName, final KvStateID kvStateId, final InetSocketAddress kvStateServerAddress) throws FlinkJobNotFoundException {
		if (jobGraph.getJobID().equals(jobId)) {
			if (log.isDebugEnabled()) {
				log.debug("Key value state registered for job {} under name {}.",
					jobGraph.getJobID(), registrationName);
			}

			try {
				executionGraph.getKvStateLocationRegistry().notifyKvStateRegistered(
					jobVertexId, keyGroupRange, registrationName, kvStateId, kvStateServerAddress);
			} catch (Exception e) {
				log.error("Failed to notify KvStateRegistry about registration {}.", registrationName, e);
				throw new RuntimeException(e);
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Notification about key-value state registration for unknown job {} received.", jobId);
			}
			throw new FlinkJobNotFoundException(jobId);
		}
	}

	@Override
	public void notifyKvStateUnregistered(final JobID jobId, final JobVertexID jobVertexId, final KeyGroupRange keyGroupRange, final String registrationName) throws FlinkJobNotFoundException {
		if (jobGraph.getJobID().equals(jobId)) {
			if (log.isDebugEnabled()) {
				log.debug("Key value state unregistered for job {} under name {}.",
					jobGraph.getJobID(), registrationName);
			}

			try {
				executionGraph.getKvStateLocationRegistry().notifyKvStateUnregistered(
					jobVertexId, keyGroupRange, registrationName);
			} catch (Exception e) {
				log.error("Failed to notify KvStateRegistry about unregistration {}.", registrationName, e);
				throw new RuntimeException(e);
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Notification about key-value state deregistration for unknown job {} received.", jobId);
			}
			throw new FlinkJobNotFoundException(jobId);
		}
	}

	@Override
	public void updateAccumulators(final AccumulatorSnapshot accumulatorSnapshot) {
		executionGraph.updateAccumulators(accumulatorSnapshot);
	}

	@Override
	public Optional<OperatorBackPressureStats> requestOperatorBackPressureStats(final JobVertexID jobVertexId) throws FlinkException {
		final ExecutionJobVertex jobVertex = executionGraph.getJobVertex(jobVertexId);
		if (jobVertex == null) {
			throw new FlinkException("JobVertexID not found " +
				jobVertexId);
		}

		return backPressureStatsTracker.getOperatorBackPressureStats(jobVertex);
	}

	@Override
	public ArchivedExecutionGraph requestJob() {
		return ArchivedExecutionGraph.createFrom(executionGraph);
	}

	@Override
	public JobStatus requestJobStatus() {
		return executionGraph.getState();
	}

	@Override
	public JobDetails requestJobDetails() {
		return WebMonitorUtils.createDetailsForJob(executionGraph);
	}

	@Override
	public CompletableFuture<String> triggerSavepoint(final String targetDirectory, final boolean cancelJob) {
		final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
		if (checkpointCoordinator == null) {
			throw new IllegalStateException(
				String.format("Job %s is not a streaming job.", jobGraph.getJobID()));
		} else if (targetDirectory == null && !checkpointCoordinator.getCheckpointStorage().hasDefaultSavepointLocation()) {
			log.info("Trying to cancel job {} with savepoint, but no savepoint directory configured.", jobGraph.getJobID());

			throw new IllegalStateException(
				"No savepoint directory configured. You can either specify a directory " +
					"while cancelling via -s :targetDirectory or configure a cluster-wide " +
					"default via key '" + CheckpointingOptions.SAVEPOINT_DIRECTORY.key() + "'.");
		}

		if (cancelJob) {
			checkpointCoordinator.stopCheckpointScheduler();
		}

		return checkpointCoordinator
			.triggerSavepoint(System.currentTimeMillis(), targetDirectory)
			.thenApply(CompletedCheckpoint::getExternalPointer)
			.handleAsync((path, throwable) -> {
				if (throwable != null) {
					if (cancelJob) {
						startCheckpointScheduler(checkpointCoordinator);
					}
					throw new CompletionException(throwable);
				} else if (cancelJob) {
					log.info("Savepoint stored in {}. Now cancelling {}.", path, jobGraph.getJobID());
					cancel();
				}
				return path;
			}, mainThreadExecutor);
	}



	private void startCheckpointScheduler(final CheckpointCoordinator checkpointCoordinator) {
		if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
			try {
				checkpointCoordinator.startCheckpointScheduler();
			} catch (IllegalStateException ignored) {
				// Concurrent shut down of the coordinator
			}
		}
	}

	@Override
	public void setMainThreadExecutor(final ComponentMainThreadExecutor mainThreadExecutor) {
		this.mainThreadExecutor = checkNotNull(mainThreadExecutor);
	}

	@Override
	public void acknowledgeCheckpoint(final JobID jobID, final ExecutionAttemptID executionAttemptID, final long checkpointId, final CheckpointMetrics checkpointMetrics, final TaskStateSnapshot checkpointState) {
		final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
		final AcknowledgeCheckpoint ackMessage = new AcknowledgeCheckpoint(
			jobID,
			executionAttemptID,
			checkpointId,
			checkpointMetrics,
			checkpointState);

		if (checkpointCoordinator != null) {
			ioExecutor.execute(() -> {
				try {
					checkpointCoordinator.receiveAcknowledgeMessage(ackMessage);
				} catch (Throwable t) {
					log.warn("Error while processing checkpoint acknowledgement message", t);
				}
			});
		} else {
			String errorMessage = "Received AcknowledgeCheckpoint message for job {} with no CheckpointCoordinator";
			if (executionGraph.getState() == JobStatus.RUNNING) {
				log.error(errorMessage, jobGraph.getJobID());
			} else {
				log.debug(errorMessage, jobGraph.getJobID());
			}
		}
	}

	@Override
	public void declineCheckpoint(final DeclineCheckpoint decline) {
		final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();

		if (checkpointCoordinator != null) {
			ioExecutor.execute(() -> {
				try {
					checkpointCoordinator.receiveDeclineMessage(decline);
				} catch (Exception e) {
					log.error("Error in CheckpointCoordinator while processing {}", decline, e);
				}
			});
		} else {
			String errorMessage = "Received DeclineCheckpoint message for job {} with no CheckpointCoordinator";
			if (executionGraph.getState() == JobStatus.RUNNING) {
				log.error(errorMessage, jobGraph.getJobID());
			} else {
				log.debug(errorMessage, jobGraph.getJobID());
			}
		}
	}

	@Override
	public void registerJobStatusListener(final JobStatusListener jobStatusListener) {
		executionGraph.registerJobStatusListener(jobStatusListener);
	}

	@Override
	public CompletableFuture<String> stopWithSavepoint(final String targetDirectory, final boolean advanceToEndOfEventTime) {
		final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();

		if (checkpointCoordinator == null) {
			return FutureUtils.completedExceptionally(new IllegalStateException(
				String.format("Job %s is not a streaming job.", jobGraph.getJobID())));
		}

		if (targetDirectory == null && !checkpointCoordinator.getCheckpointStorage().hasDefaultSavepointLocation()) {
			log.info("Trying to cancel job {} with savepoint, but no savepoint directory configured.", jobGraph.getJobID());

			return FutureUtils.completedExceptionally(new IllegalStateException(
				"No savepoint directory configured. You can either specify a directory " +
					"while cancelling via -s :targetDirectory or configure a cluster-wide " +
					"default via key '" + CheckpointingOptions.SAVEPOINT_DIRECTORY.key() + "'."));
		}

		final long now = System.currentTimeMillis();

		// we stop the checkpoint coordinator so that we are guaranteed
		// to have only the data of the synchronous savepoint committed.
		// in case of failure, and if the job restarts, the coordinator
		// will be restarted by the CheckpointCoordinatorDeActivator.
		checkpointCoordinator.stopCheckpointScheduler();

		final CompletableFuture<String> savepointFuture = checkpointCoordinator
			.triggerSynchronousSavepoint(now, advanceToEndOfEventTime, targetDirectory)
			.handleAsync((completedCheckpoint, throwable) -> {
				if (throwable != null) {
					log.info("Failed during stopping job {} with a savepoint. Reason: {}", jobGraph.getJobID(), throwable.getMessage());
					throw new CompletionException(throwable);
				}
				return completedCheckpoint.getExternalPointer();
			}, mainThreadExecutor);

		final CompletableFuture<JobStatus> terminationFuture = executionGraph
			.getTerminationFuture()
			.handleAsync((jobstatus, throwable) -> {

				if (throwable != null) {
					log.info("Failed during stopping job {} with a savepoint. Reason: {}", jobGraph.getJobID(), throwable.getMessage());
					throw new CompletionException(throwable);
				} else if(jobstatus != JobStatus.FINISHED) {
					log.info("Failed during stopping job {} with a savepoint. Reason: Reached state {} instead of FINISHED.", jobGraph.getJobID(), jobstatus);
					throw new CompletionException(new FlinkException("Reached state " + jobstatus + " instead of FINISHED."));
				}
				return jobstatus;
			}, mainThreadExecutor);

		return savepointFuture.thenCompose((path) ->
			terminationFuture.thenApply((jobStatus -> path)));
	}
}
