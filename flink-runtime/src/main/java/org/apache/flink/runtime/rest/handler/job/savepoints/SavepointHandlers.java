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

package org.apache.flink.runtime.rest.handler.job.savepoints;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rest.NotFoundException;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.SavepointTriggerIdPathParameter;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointInfo;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointResponseBody;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointStatusHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerId;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerRequestBody;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerResponseBody;
import org.apache.flink.runtime.rest.messages.queue.QueueStatus;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.types.Either;
import org.apache.flink.util.SerializedThrowable;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * HTTP handlers for asynchronous triggering of savepoints.
 */
public class SavepointHandlers {

	private final CompletedCheckpointCache completedCheckpointCache = new CompletedCheckpointCache();

	@Nullable
	private final String defaultSavepointDir;

	public SavepointHandlers(@Nullable final String defaultSavepointDir) {
		this.defaultSavepointDir = defaultSavepointDir;
	}

	/**
	 * HTTP handler to trigger savepoints.
	 */
	public class SavepointTriggerHandler
			extends AbstractRestHandler<RestfulGateway, SavepointTriggerRequestBody, SavepointTriggerResponseBody, SavepointTriggerMessageParameters> {

		public SavepointTriggerHandler(
				final CompletableFuture<String> localRestAddress,
				final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
				final Time timeout,
				final Map<String, String> responseHeaders) {
			super(localRestAddress, leaderRetriever, timeout, responseHeaders, SavepointTriggerHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<SavepointTriggerResponseBody> handleRequest(
				@Nonnull final HandlerRequest<SavepointTriggerRequestBody, SavepointTriggerMessageParameters> request,
				@Nonnull final RestfulGateway gateway) throws RestHandlerException {

			final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
			final String requestedTargetDirectory = request.getRequestBody().getTargetDirectory();

			if (requestedTargetDirectory == null && defaultSavepointDir == null) {
				return FutureUtils.completedExceptionally(
					new RestHandlerException(
						String.format("Savepoint directory could not be determined. " +
								"Set property [%s] or set config key [%s].",
							SavepointTriggerRequestBody.FIELD_NAME_TARGET_DIRECTORY,
							CoreOptions.SAVEPOINT_DIRECTORY.key()),
						HttpResponseStatus.BAD_REQUEST));
			}

			final String targetDirectory = requestedTargetDirectory != null ? requestedTargetDirectory : defaultSavepointDir;
			final CompletableFuture<CompletedCheckpoint> completedCheckpointCompletableFuture =
				gateway.triggerSavepoint(jobId, targetDirectory, RpcUtils.INF_TIMEOUT);
			final SavepointTriggerId savepointTriggerId = new SavepointTriggerId();
			completedCheckpointCache.registerOngoingCheckpoint(
				SavepointKey.of(savepointTriggerId, jobId),
				completedCheckpointCompletableFuture);
			return CompletableFuture.completedFuture(
				new SavepointTriggerResponseBody(savepointTriggerId));
		}
	}

	/**
	 * HTTP handler to query for the status of the savepoint.
	 */
	public class SavepointStatusHandler
			extends AbstractRestHandler<RestfulGateway, EmptyRequestBody, SavepointResponseBody, SavepointStatusMessageParameters> {

		public SavepointStatusHandler(
				final CompletableFuture<String> localRestAddress,
				final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
				final Time timeout,
				final Map<String, String> responseHeaders) {
			super(localRestAddress, leaderRetriever, timeout, responseHeaders, SavepointStatusHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<SavepointResponseBody> handleRequest(
				@Nonnull final HandlerRequest<EmptyRequestBody, SavepointStatusMessageParameters> request,
				@Nonnull final RestfulGateway gateway) throws RestHandlerException {

			final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
			final SavepointTriggerId savepointTriggerId = request.getPathParameter(
				SavepointTriggerIdPathParameter.class);
			@Nullable final CompletedCheckpoint completedCheckpoint;
			try {
				completedCheckpoint = completedCheckpointCache.get(
					SavepointKey.of(savepointTriggerId, jobId));
			} catch (UnknownSavepointTriggerId e) {
				return FutureUtils.completedExceptionally(
					new NotFoundException("Savepoint not found. Savepoint trigger id: " +
						savepointTriggerId +
						", job id: " + jobId));
			} catch (Throwable throwable) {
				return CompletableFuture.completedFuture(new SavepointResponseBody(
					QueueStatus.completed(),
					new SavepointInfo(savepointTriggerId, null, new SerializedThrowable(throwable))));
			}

			if (completedCheckpoint != null) {
				final String externalPointer = completedCheckpoint.getExternalPointer();
				checkState(externalPointer != null, "External pointer must not be null");
				return CompletableFuture.completedFuture(new SavepointResponseBody(
					QueueStatus.completed(),
					new SavepointInfo(savepointTriggerId, externalPointer, null)));
			} else {
				return CompletableFuture.completedFuture(SavepointResponseBody.inProgress());
			}
		}
	}

	/**
	 * Cache to manage ongoing checkpoints.
	 *
	 * <p>Completed checkpoints will be removed from the cache automatically after a fixed timeout.
	 */
	@ThreadSafe
	static class CompletedCheckpointCache {

		/**
		 * Stores SavepointKeys of ongoing checkpoints.
		 * If the checkpoint completes, it will be moved to {@link #completedCheckpoints}.
		 */
		private final Set<SavepointKey> registeredSavepointTriggers = ConcurrentHashMap.newKeySet();

		/** Caches completed checkpoints. */
		private final Cache<SavepointKey, Either<Throwable, CompletedCheckpoint>> completedCheckpoints =
			CacheBuilder.newBuilder()
				.expireAfterWrite(300, TimeUnit.SECONDS)
				.build();

		/**
		 * Registers an ongoing checkpoint with the cache.
		 */
		void registerOngoingCheckpoint(
				final SavepointKey savepointTriggerId,
				final CompletableFuture<CompletedCheckpoint> checkpointFuture) {
			registeredSavepointTriggers.add(savepointTriggerId);
			checkpointFuture.whenComplete((completedCheckpoint, error) -> {
				if (error == null) {
					completedCheckpoints.put(savepointTriggerId, Either.Right(completedCheckpoint));
				} else {
					completedCheckpoints.put(savepointTriggerId, Either.Left(error));
				}
				registeredSavepointTriggers.remove(savepointTriggerId);
			});
		}

		/**
		 * Returns the CompletedCheckpoint if ready, otherwise {@code null}.
		 *
		 * @throws UnknownSavepointTriggerId If the savepoint is not found, and there is no ongoing
		 *                                   checkpoint under the provided key.
		 * @throws Throwable                 If the savepoint completed with an exception.
		 */
		@Nullable
		CompletedCheckpoint get(final SavepointKey savepointTriggerId)
				throws UnknownSavepointTriggerId, Throwable {
			Either<Throwable, CompletedCheckpoint> completedCheckpointOrError = null;
			if (!registeredSavepointTriggers.contains(savepointTriggerId)
				&& (completedCheckpointOrError = completedCheckpoints.getIfPresent(savepointTriggerId)) == null) {
				throw new UnknownSavepointTriggerId();
			}

			if (completedCheckpointOrError == null) {
				return null;
			} else if (completedCheckpointOrError.isLeft()) {
				throw completedCheckpointOrError.left();
			} else {
				final CompletedCheckpoint completedCheckpoint = completedCheckpointOrError.right();
				if (completedCheckpoint.getExternalPointer() == null) {
					throw new RuntimeException("Savepoint has not been persisted.");
				}
				return completedCheckpoint;
			}
		}
	}

	static class SavepointKey {

		private final SavepointTriggerId savepointTriggerId;

		private final JobID jobId;

		private SavepointKey(final SavepointTriggerId savepointTriggerId, final JobID jobId) {
			this.savepointTriggerId = requireNonNull(savepointTriggerId);
			this.jobId = requireNonNull(jobId);
		}

		private static SavepointKey of(final SavepointTriggerId savepointTriggerId, final JobID jobId) {
			return new SavepointKey(savepointTriggerId, jobId);
		}

		@Override
		public boolean equals(final Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			final SavepointKey that = (SavepointKey) o;

			if (!savepointTriggerId.equals(that.savepointTriggerId)) {
				return false;
			}
			return jobId.equals(that.jobId);
		}

		@Override
		public int hashCode() {
			int result = savepointTriggerId.hashCode();
			result = 31 * result + jobId.hashCode();
			return result;
		}
	}

	static class UnknownSavepointTriggerId extends Exception {
		private static final long serialVersionUID = 1L;
	}
}
