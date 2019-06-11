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
import org.apache.flink.runtime.executiongraph.failover.flip1.NeverRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.TestRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.TestingAbstractInvokables;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.VoidBackPressureStatsTracker;
import org.apache.flink.runtime.scheduler.strategy.EagerSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link DefaultScheduler}.
 */
public class DefaultSchedulerTest extends TestLogger {

	private static final int TIMEOUT_MS = 1000;

	private ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor = new ManuallyTriggeredScheduledExecutor();

	@Test
	public void startScheduling() throws Exception {
		final JobGraph jobGraph = new JobGraph();
		final JobVertex vertex = new JobVertex("source");
		vertex.setInvokableClass(TestingAbstractInvokables.Sender.class);
		jobGraph.addVertex(vertex);

		final Executor executor = Executors.newSingleThreadExecutor();
		final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

		final Configuration configuration = new Configuration();
		configuration.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "throwing");

		SubmissionTrackingTaskManagerGateway taskManagerGateway = new SubmissionTrackingTaskManagerGateway();
		final DefaultScheduler scheduler = new DefaultScheduler(
			log,
			jobGraph,
			VoidBackPressureStatsTracker.INSTANCE,
			executor,
			configuration,
			new SimpleSlotProvider(jobGraph.getJobID(), 12, taskManagerGateway),
			scheduledExecutorService,
			manuallyTriggeredScheduledExecutor,
			ClassLoader.getSystemClassLoader(),
			new StandaloneCheckpointRecoveryFactory(),
			Time.seconds(300),
			VoidBlobWriter.getInstance(),
			UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup(),
			Time.seconds(300),
			new EagerSchedulingStrategy.Factory(),
			new RestartAllTasksFailoverStrategy.Factory(),
			NeverRestartBackoffTimeStrategy.INSTANCE);

		scheduler.startScheduling();

		final List<ExecutionVertexID> deployedExecutionVertices = taskManagerGateway.getDeployedExecutionVertices(1, TIMEOUT_MS);
		assertThat(deployedExecutionVertices, contains(new ExecutionVertexID(vertex.getID(), 0)));
	}

	@Test
	public void updateTaskExecutionState() throws Exception {
		final JobGraph jobGraph = new JobGraph();
		final JobVertex vertex = new JobVertex("source");
		vertex.setInvokableClass(TestingAbstractInvokables.Sender.class);
		jobGraph.addVertex(vertex);

		final Executor executor = Executors.newSingleThreadExecutor();
		final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

		final Configuration configuration = new Configuration();
		configuration.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "throwing");

		SubmissionTrackingTaskManagerGateway taskManagerGateway = new SubmissionTrackingTaskManagerGateway();

		final DefaultScheduler scheduler = new DefaultScheduler(
			log,
			jobGraph,
			VoidBackPressureStatsTracker.INSTANCE,
			executor,
			configuration,
			new SimpleSlotProvider(jobGraph.getJobID(), 12, taskManagerGateway),
			scheduledExecutorService,
			manuallyTriggeredScheduledExecutor,
			ClassLoader.getSystemClassLoader(),
			new StandaloneCheckpointRecoveryFactory(),
			Time.seconds(300),
			VoidBlobWriter.getInstance(),
			UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup(),
			Time.seconds(300),
			new EagerSchedulingStrategy.Factory(),
			new RestartAllTasksFailoverStrategy.Factory(),
			new TestRestartBackoffTimeStrategy(true, 0));

		scheduler.setMainThreadExecutor(TestingComponentMainThreadExecutorServiceAdapter.forMainThread());
		scheduler.startScheduling();

		final ArchivedExecutionVertex next = scheduler.requestJob().getAllExecutionVertices().iterator().next();
		final ExecutionAttemptID attemptId = next.getCurrentExecutionAttempt().getAttemptId();

		scheduler.updateTaskExecutionState(new TaskExecutionState(jobGraph.getJobID(), attemptId, ExecutionState.FAILED));

		manuallyTriggeredScheduledExecutor.triggerScheduledTasks();

		final List<ExecutionVertexID> deployedExecutionVertices = taskManagerGateway.getDeployedExecutionVertices(2, TIMEOUT_MS);
		assertThat(deployedExecutionVertices, contains(new ExecutionVertexID(vertex.getID(), 0), new ExecutionVertexID(vertex.getID(), 0)));
	}

	@Test
	public void failJobIfCannotRestart() throws Exception {
		final JobGraph jobGraph = new JobGraph();
		final JobVertex vertex = new JobVertex("source");
		vertex.setInvokableClass(TestingAbstractInvokables.Sender.class);
		jobGraph.addVertex(vertex);

		final Executor executor = Executors.newSingleThreadExecutor();
		final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

		final Configuration configuration = new Configuration();
		configuration.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "throwing");

		SubmissionTrackingTaskManagerGateway taskManagerGateway = new SubmissionTrackingTaskManagerGateway();

		final DefaultScheduler scheduler = new DefaultScheduler(
			log,
			jobGraph,
			VoidBackPressureStatsTracker.INSTANCE,
			executor,
			configuration,
			new SimpleSlotProvider(jobGraph.getJobID(), 12, taskManagerGateway),
			scheduledExecutorService,
			manuallyTriggeredScheduledExecutor,
			ClassLoader.getSystemClassLoader(),
			new StandaloneCheckpointRecoveryFactory(),
			Time.seconds(300),
			VoidBlobWriter.getInstance(),
			UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup(),
			Time.seconds(300),
			new EagerSchedulingStrategy.Factory(),
			new RestartAllTasksFailoverStrategy.Factory(),
			new TestRestartBackoffTimeStrategy(false, 0));

		scheduler.setMainThreadExecutor(TestingComponentMainThreadExecutorServiceAdapter.forMainThread());
		scheduler.startScheduling();

		final ArchivedExecutionVertex next = scheduler.requestJob().getAllExecutionVertices().iterator().next();
		final ExecutionAttemptID attemptId = next.getCurrentExecutionAttempt().getAttemptId();

		scheduler.updateTaskExecutionState(new TaskExecutionState(jobGraph.getJobID(), attemptId, ExecutionState.FAILED));

		manuallyTriggeredScheduledExecutor.triggerScheduledTasks();

		scheduler.getTerminationFuture().get();
		final JobStatus jobStatus = scheduler.requestJobStatus();
		assertThat(jobStatus, is(equalTo(JobStatus.FAILED)));
	}
}
