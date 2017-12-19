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

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.rest.messages.json.JobIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobIDSerializer;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.SerializedValue;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * This class is used to represent the information in {@link JobExecutionResult} as JSON. In case
 * of a job failure, no {@link JobExecutionResult} will be available. In this case instances of this
 * class will only store a {@link Throwable}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JobExecutionResult {

	private static final String FIELD_NAME_JOB_ID = "id";

	private static final String FIELD_NAME_NET_RUNTIME = "net-runtime";

	private static final String FIELD_NAME_ACCUMULATOR_RESULTS = "accumulator-results";

	private static final String FIELD_NAME_FAILURE_CAUSE = "failure-cause";

	@JsonSerialize(using = JobIDSerializer.class)
	@JsonDeserialize(using = JobIDDeserializer.class)
	@JsonProperty(value = FIELD_NAME_JOB_ID, required = true)
	private final JobID jobId;

	@JsonProperty(FIELD_NAME_NET_RUNTIME)
	private final long netRuntime;

	@JsonProperty(FIELD_NAME_ACCUMULATOR_RESULTS)
	private final Map<String, SerializedValue<Object>> accumulatorResults;

	@JsonProperty(FIELD_NAME_FAILURE_CAUSE)
	private final SerializedThrowable failureCause;

	@JsonCreator
	public JobExecutionResult(
		@JsonDeserialize(using = JobIDDeserializer.class)
		@JsonProperty(value = FIELD_NAME_JOB_ID, required = true) final JobID jobId,
		@Nullable @JsonProperty(FIELD_NAME_NET_RUNTIME) final long netRuntime,
		@Nullable @JsonProperty(FIELD_NAME_ACCUMULATOR_RESULTS) final Map<String, SerializedValue<Object>> accumulatorResults,
		@Nullable @JsonProperty(FIELD_NAME_FAILURE_CAUSE) final SerializedThrowable failureCause) throws IOException {
		this.jobId = requireNonNull(jobId);
		this.netRuntime = netRuntime;
		this.accumulatorResults = accumulatorResults;
		this.failureCause = failureCause;
	}

	public static JobExecutionResult from(final org.apache.flink.runtime.jobmaster.JobExecutionResult jobExecutionResult) throws IOException {
		return new JobExecutionResult(
			jobExecutionResult.getJobId(),
			jobExecutionResult.getNetRuntime(),
			jobExecutionResult.getAccumulatorResults(),
			jobExecutionResult.getSerializedThrowable().orElse(null));
	}

	public JobID getJobId() {
		return jobId;
	}

	public long getNetRuntime() {
		return netRuntime;
	}

	@Nullable
	public Map<String, SerializedValue<Object>> getAccumulatorResults() {
		return accumulatorResults;
	}

	@Nullable
	public SerializedThrowable getFailureCause() {
		return failureCause;
	}

}
