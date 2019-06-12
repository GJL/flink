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
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.TestingComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.executiongraph.failover.flip1.TestRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.VoidBackPressureStatsTracker;
import org.apache.flink.runtime.scheduler.strategy.EagerSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link DefaultScheduler}.
 */
public class DefaultSchedulerTest extends TestLogger {

	private static final int TIMEOUT_MS = 1000;

	private ManuallyTriggeredScheduledExecutor taskRestartExecutor = new ManuallyTriggeredScheduledExecutor();

	private Executor executor;

	private ScheduledExecutorService scheduledExecutorService;

	private Configuration configuration;

	private SubmissionTrackingTaskManagerGateway taskManagerGateway;

	private TestRestartBackoffTimeStrategy testRestartBackoffTimeStrategy;

	@Before
	public void setUp() throws Exception {
		executor = Executors.newSingleThreadExecutor();
		scheduledExecutorService = new DirectScheduledExecutorService();

		configuration = new Configuration();
		configuration.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "throwing");
		taskManagerGateway = new SubmissionTrackingTaskManagerGateway();

		testRestartBackoffTimeStrategy = new TestRestartBackoffTimeStrategy(true, 0);
	}

	@Test
	public void startScheduling() {
		final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
		final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);

		createSchedulerAndStartScheduling(jobGraph);

		final List<ExecutionVertexID> deployedExecutionVertices = taskManagerGateway.getDeployedExecutionVertices(1, TIMEOUT_MS);

		final ExecutionVertexID executionVertexId = new ExecutionVertexID(onlyJobVertex.getID(), 0);
		assertThat(deployedExecutionVertices, contains(executionVertexId));
	}

	@Test
	public void updateTaskExecutionState() {
		final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
		final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);

		final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

		final ArchivedExecutionVertex archivedExecutionVertex = scheduler.requestJob().getAllExecutionVertices().iterator().next();
		final ExecutionAttemptID attemptId = archivedExecutionVertex.getCurrentExecutionAttempt().getAttemptId();

		scheduler.updateTaskExecutionState(new TaskExecutionState(jobGraph.getJobID(), attemptId, ExecutionState.FAILED));

		taskRestartExecutor.triggerScheduledTasks();

		final List<ExecutionVertexID> deployedExecutionVertices = taskManagerGateway.getDeployedExecutionVertices(2, TIMEOUT_MS);
		final ExecutionVertexID executionVertexID = new ExecutionVertexID(onlyJobVertex.getID(), 0);
		assertThat(deployedExecutionVertices, contains(executionVertexID, executionVertexID));
	}

	@Test
	public void updateTaskExecutionStateReturnsFalseIfExecutionDoesNotExist() {
		final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
		final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

		final TaskExecutionState taskExecutionState = new TaskExecutionState(
			jobGraph.getJobID(),
			new ExecutionAttemptID(),
			ExecutionState.FAILED);

		assertFalse(scheduler.updateTaskExecutionState(taskExecutionState));
	}

	@Test
	public void failJobIfCannotRestart() throws Exception {
		final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
		testRestartBackoffTimeStrategy.setCanRestart(false);

		final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

		final ArchivedExecutionVertex onlyExecutionVertex = scheduler.requestJob().getAllExecutionVertices().iterator().next();
		final ExecutionAttemptID attemptId = onlyExecutionVertex.getCurrentExecutionAttempt().getAttemptId();

		scheduler.updateTaskExecutionState(new TaskExecutionState(jobGraph.getJobID(), attemptId, ExecutionState.FAILED));

		taskRestartExecutor.triggerScheduledTasks();

		waitForTermination(scheduler);
		final JobStatus jobStatus = scheduler.requestJobStatus();
		assertThat(jobStatus, is(Matchers.equalTo(JobStatus.FAILED)));
	}

	private void waitForTermination(final DefaultScheduler scheduler) throws Exception {
		scheduler.getTerminationFuture().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
	}

	private static JobGraph singleNonParallelJobVertexJobGraph() {
		final JobGraph jobGraph = new JobGraph();
		final JobVertex vertex = new JobVertex("source");
		vertex.setInvokableClass(NoOpInvokable.class);
		jobGraph.addVertex(vertex);
		return jobGraph;
	}

	private static JobVertex getOnlyJobVertex(final JobGraph jobGraph) {
		final List<JobVertex> sortedVertices = jobGraph.getVerticesSortedTopologicallyFromSources();
		Preconditions.checkState(sortedVertices.size() == 1);
		return sortedVertices.get(0);
	}

	private DefaultScheduler createSchedulerAndStartScheduling(final JobGraph jobGraph) {
		try {
			final DefaultScheduler scheduler = createScheduler(jobGraph);
			startScheduling(scheduler);
			return scheduler;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private DefaultScheduler createScheduler(final JobGraph jobGraph) throws Exception {
		return new DefaultScheduler(
			log,
			jobGraph,
			VoidBackPressureStatsTracker.INSTANCE,
			executor,
			configuration,
			new SimpleSlotProvider(jobGraph.getJobID(), 12, taskManagerGateway),
			scheduledExecutorService,
			taskRestartExecutor,
			ClassLoader.getSystemClassLoader(),
			new StandaloneCheckpointRecoveryFactory(),
			Time.seconds(300),
			VoidBlobWriter.getInstance(),
			UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup(),
			Time.seconds(300),
			new EagerSchedulingStrategy.Factory(),
			new RestartAllTasksFailoverStrategy.Factory(),
			testRestartBackoffTimeStrategy);
	}

	private void startScheduling(final SchedulerNG scheduler) {
		scheduler.setMainThreadExecutor(TestingComponentMainThreadExecutorServiceAdapter.forMainThread());
		scheduler.startScheduling();
	}

}
