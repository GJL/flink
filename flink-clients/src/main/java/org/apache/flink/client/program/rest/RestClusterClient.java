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

package org.apache.flink.client.program.rest;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.program.rest.retry.ExponentialWaitStrategy;
import org.apache.flink.client.program.rest.retry.WaitStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.client.SerializedJobExecutionResult;
import org.apache.flink.runtime.clusterframework.messages.GetClusterStatusResponse;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobExecutionResult;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.messages.BlobServerPortHeaders;
import org.apache.flink.runtime.rest.messages.BlobServerPortResponseBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.JobTerminationHeaders;
import org.apache.flink.runtime.rest.messages.JobTerminationMessageParameters;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.TerminationModeQueryParameter;
import org.apache.flink.runtime.rest.messages.job.JobExecutionResultHeaders;
import org.apache.flink.runtime.rest.messages.job.JobSubmitHeaders;
import org.apache.flink.runtime.rest.messages.job.JobSubmitRequestBody;
import org.apache.flink.runtime.rest.messages.job.JobSubmitResponseBody;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointInfo;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointStatusHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerId;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerRequestBody;
import org.apache.flink.runtime.rest.messages.queue.AsynchronouslyCreatedResource;
import org.apache.flink.runtime.rest.messages.queue.QueueStatus;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * A {@link ClusterClient} implementation that communicates via HTTP REST requests.
 */
public class RestClusterClient extends ClusterClient {

	private final RestClusterClientConfiguration restClusterClientConfiguration;
	private final RestClient restClient;
	private final ExecutorService executorService = Executors.newFixedThreadPool(4, new ExecutorThreadFactory("Flink-RestClusterClient-IO"));
	private final WaitStrategy waitStrategy;

	public RestClusterClient(Configuration config) throws Exception {
		this(config, new ExponentialWaitStrategy(10, 2000));
	}

	@VisibleForTesting
	RestClusterClient(Configuration configuration, WaitStrategy waitStrategy) throws Exception {
		super(configuration);
		this.restClusterClientConfiguration = RestClusterClientConfiguration.fromConfiguration(configuration);
		this.restClient = new RestClient(restClusterClientConfiguration.getRestClientConfiguration(), executorService);
		this.waitStrategy = requireNonNull(waitStrategy);
	}

	@Override
	public void shutdown() {
		try {
			// we only call this for legacy reasons to shutdown components that are started in the ClusterClient constructor
			super.shutdown();
		} catch (Exception e) {
			log.error("An error occurred during the client shutdown.", e);
		}
		this.restClient.shutdown(Time.seconds(5));
		ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, this.executorService);
	}

	@Override
	protected JobSubmissionResult submitJob(JobGraph jobGraph, ClassLoader classLoader) throws ProgramInvocationException {
		log.info("Submitting job.");
		try {
			// temporary hack for FLIP-6 since slot-sharing isn't implemented yet
			jobGraph.setAllowQueuedScheduling(true);
			submitJob(jobGraph);
		} catch (JobSubmissionException e) {
			throw new ProgramInvocationException(e);
		}

		final JobExecutionResult jobExecutionResult;
		try {
			jobExecutionResult = waitForResource(
				() -> {
					final JobMessageParameters messageParameters = new JobMessageParameters();
					messageParameters.jobPathParameter.resolve(jobGraph.getJobID());
					return restClient.sendRequest(
						restClusterClientConfiguration.getRestServerAddress(),
						restClusterClientConfiguration.getRestServerPort(),
						JobExecutionResultHeaders.getInstance(),
						messageParameters);
				});
		} catch (final Exception e) {
			throw new ProgramInvocationException(e);
		}

		if (jobExecutionResult.getSerializedThrowable().isPresent()) {
			final SerializedThrowable serializedThrowable = jobExecutionResult.getSerializedThrowable().get();
			final Throwable throwable = serializedThrowable.deserializeError(classLoader);
			throw new ProgramInvocationException(throwable);
		}

		try {
			// don't return just a JobSubmissionResult here, the signature is lying
			// The CliFrontend expects this to be a JobExecutionResult
			this.lastJobExecutionResult = new SerializedJobExecutionResult(
				jobExecutionResult.getJobId(),
				jobExecutionResult.getNetRuntime(),
				jobExecutionResult.getAccumulatorResults()).toJobExecutionResult(classLoader);
			return lastJobExecutionResult;
		} catch (IOException | ClassNotFoundException e) {
			throw new ProgramInvocationException(e);
		}
	}

	private void submitJob(JobGraph jobGraph) throws JobSubmissionException {
		log.info("Requesting blob server port.");
		int blobServerPort;
		try {
			CompletableFuture<BlobServerPortResponseBody> portFuture = restClient.sendRequest(
				restClusterClientConfiguration.getRestServerAddress(),
				restClusterClientConfiguration.getRestServerPort(),
				BlobServerPortHeaders.getInstance());
			blobServerPort = portFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS).port;
		} catch (Exception e) {
			throw new JobSubmissionException(jobGraph.getJobID(), "Failed to retrieve blob server port.", e);
		}

		log.info("Uploading jar files.");
		try {
			InetSocketAddress address = new InetSocketAddress(restClusterClientConfiguration.getBlobServerAddress(), blobServerPort);
			List<PermanentBlobKey> keys = BlobClient.uploadJarFiles(address, this.flinkConfig, jobGraph.getJobID(), jobGraph.getUserJars());
			for (PermanentBlobKey key : keys) {
				jobGraph.addBlob(key);
			}
		} catch (Exception e) {
			throw new JobSubmissionException(jobGraph.getJobID(), "Failed to upload user jars to blob server.", e);
		}

		log.info("Submitting job graph.");
		try {
			CompletableFuture<JobSubmitResponseBody> responseFuture = restClient.sendRequest(
				restClusterClientConfiguration.getRestServerAddress(),
				restClusterClientConfiguration.getRestServerPort(),
				JobSubmitHeaders.getInstance(),
				new JobSubmitRequestBody(jobGraph));
			responseFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			throw new JobSubmissionException(jobGraph.getJobID(), "Failed to submit JobGraph.", e);
		}
	}

	private <R, T extends AsynchronouslyCreatedResource<R> & ResponseBody> R waitForResource(
			final SupplierWithException<CompletableFuture<T>, IOException> supplier) throws Exception {
		T asynchronouslyCreatedResource;
		long attempt = 0;
		do {
			final CompletableFuture<T> responseFuture = supplier.get();
			asynchronouslyCreatedResource = responseFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
			Thread.sleep(waitStrategy.sleepTime(attempt));
			attempt++;
		}
		while (asynchronouslyCreatedResource.queueStatus().getStatusId() != QueueStatus.StatusId.COMPLETED);
		return asynchronouslyCreatedResource.resource();
	}

	@Override
	public void stop(JobID jobID) throws Exception {
		JobTerminationMessageParameters params = new JobTerminationMessageParameters();
		params.jobPathParameter.resolve(jobID);
		params.terminationModeQueryParameter.resolve(Collections.singletonList(TerminationModeQueryParameter.TerminationMode.STOP));
		CompletableFuture<EmptyResponseBody> responseFuture = restClient.sendRequest(
			restClusterClientConfiguration.getRestServerAddress(),
			restClusterClientConfiguration.getRestServerPort(),
			JobTerminationHeaders.getInstance(),
			params
		);
		responseFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public void cancel(JobID jobID) throws Exception {
		JobTerminationMessageParameters params = new JobTerminationMessageParameters();
		params.jobPathParameter.resolve(jobID);
		params.terminationModeQueryParameter.resolve(Collections.singletonList(TerminationModeQueryParameter.TerminationMode.CANCEL));
		CompletableFuture<EmptyResponseBody> responseFuture = restClient.sendRequest(
			restClusterClientConfiguration.getRestServerAddress(),
			restClusterClientConfiguration.getRestServerPort(),
			JobTerminationHeaders.getInstance(),
			params
		);
		responseFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public String cancelWithSavepoint(JobID jobId, @Nullable String savepointDirectory) throws Exception {
		throw new UnsupportedOperationException("Not implemented yet.");
	}

	@Override
	public CompletableFuture<String> triggerSavepoint(
			final JobID jobId,
			final @Nullable String savepointDirectory) throws Exception {
		final SavepointTriggerHeaders savepointTriggerHeaders = SavepointTriggerHeaders.getInstance();
		final SavepointTriggerMessageParameters savepointTriggerMessageParameters =
			savepointTriggerHeaders.getUnresolvedMessageParameters();
		savepointTriggerMessageParameters.jobID.resolve(jobId);
		return restClient.sendRequest(
			restClusterClientConfiguration.getRestServerAddress(),
			restClusterClientConfiguration.getRestServerPort(),
			savepointTriggerHeaders,
			savepointTriggerMessageParameters,
			new SavepointTriggerRequestBody(savepointDirectory)
		).thenApply(savepointTriggerResponseBody -> {
			final SavepointTriggerId savepointTriggerId = savepointTriggerResponseBody.getSavepointTriggerId();
			final SavepointInfo savepointInfo;
			try {
				savepointInfo = waitForSavepointCompletion(jobId, savepointTriggerId);
			} catch (Exception e) {
				throw new CompletionException(e);
			}
			if (savepointInfo.getFailureCause() != null) {
				throw new CompletionException(savepointInfo.getFailureCause());
			}
			return savepointInfo.getLocation();
		});
	}

	private SavepointInfo waitForSavepointCompletion(
			final JobID jobId,
			final SavepointTriggerId savepointTriggerId) throws Exception {
		return waitForResource(() -> {
			final SavepointStatusHeaders savepointStatusHeaders = SavepointStatusHeaders.getInstance();
			final SavepointStatusMessageParameters savepointStatusMessageParameters =
				savepointStatusHeaders.getUnresolvedMessageParameters();
			savepointStatusMessageParameters.jobIdPathParameter.resolve(jobId);
			savepointStatusMessageParameters.savepointTriggerIdPathParameter.resolve(savepointTriggerId);
			return restClient.sendRequest(
				restClusterClientConfiguration.getRestServerAddress(),
				restClusterClientConfiguration.getRestServerPort(),
				savepointStatusHeaders,
				savepointStatusMessageParameters
			);
		});
	}

	@Override
	public CompletableFuture<Collection<JobStatusMessage>> listJobs() throws Exception {
		JobsOverviewHeaders headers = JobsOverviewHeaders.getInstance();
		CompletableFuture<MultipleJobsDetails> jobDetailsFuture = restClient.sendRequest(
			restClusterClientConfiguration.getRestServerAddress(),
			restClusterClientConfiguration.getRestServerPort(),
			headers
		);
		return jobDetailsFuture
			.thenApply(
				(MultipleJobsDetails multipleJobsDetails) -> {
					final Collection<JobDetails> jobDetails = multipleJobsDetails.getJobs();
					Collection<JobStatusMessage> flattenedDetails = new ArrayList<>(jobDetails.size());
					jobDetails.forEach(detail -> flattenedDetails.add(new JobStatusMessage(detail.getJobId(), detail.getJobName(), detail.getStatus(), detail.getStartTime())));

					return flattenedDetails;
			});
	}

	// ======================================
	// Legacy stuff we actually implement
	// ======================================

	@Override
	public String getClusterIdentifier() {
		return "Flip-6 Standalone cluster with dispatcher at " + restClusterClientConfiguration.getRestServerAddress() + '.';
	}

	@Override
	public boolean hasUserJarsInClassPath(List<URL> userJarFiles) {
		return false;
	}

	// ======================================
	// Legacy stuff we ignore
	// ======================================

	@Override
	public void waitForClusterToBeReady() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getWebInterfaceURL() {
		throw new UnsupportedOperationException();
	}

	@Override
	public GetClusterStatusResponse getClusterStatus() {
		throw new UnsupportedOperationException();
	}

	@Override
	protected List<String> getNewMessages() {
		throw new UnsupportedOperationException();
	}

	@Override
	protected void finalizeCluster() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getMaxSlots() {
		return 0;
	}
}
