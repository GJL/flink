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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.scheduler.DefaultScheduler;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Calls {@link DefaultScheduler#updateTaskExecutionState(TaskExecutionState)} on task failure.
 */
public class UpdateTaskExecutionStateInDefaultSchedulerListener implements TaskFailureListener {

	private final SchedulerNG defaultScheduler;

	private final JobID jobId;

	public UpdateTaskExecutionStateInDefaultSchedulerListener(
			final SchedulerNG defaultScheduler,
			final JobID jobId) {
		this.defaultScheduler = checkNotNull(defaultScheduler);
		this.jobId = checkNotNull(jobId);
	}

	@Override
	public void notifyFailed(final ExecutionAttemptID attemptId, final Throwable t) {
		defaultScheduler.updateTaskExecutionState(new TaskExecutionState(
			jobId,
			attemptId,
			ExecutionState.FAILED,
			t));
	}
}
