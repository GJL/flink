/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.fs2.bucketing;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.SequenceFileWriter;
import org.apache.flink.streaming.connectors.fs2.StringWriter;
import org.apache.flink.streaming.connectors.fs2.Writer;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Sink that emits its input elements to {@link FileSystem} files within
 * buckets. This is integrated with the checkpointing mechanism to provide exactly once semantics.
 *
 *
 * <p>When creating the sink a {@code basePath} must be specified. The base directory contains
 * one directory for every bucket. The bucket directories themselves contain several part files,
 * one for each parallel subtask of the sink. These part files contain the actual output data.
 *
 *
 * <p>The sink uses a {@link Bucketer} to determine in which bucket directory each element should
 * be written to inside the base directory. The {@code Bucketer} can, for example, use time or
 * a property of the element to determine the bucket directory. The default {@code Bucketer} is a
 * {@link DateTimeBucketer} which will create one new bucket every hour. You can specify
 * a custom {@code Bucketer} using {@link #setBucketer(Bucketer)}. For example, use the
 * {@link BasePathBucketer} if you don't want to have buckets but still want to write part-files
 * in a fault-tolerant way.
 *
 *
 * <p>The filenames of the part files contain the part prefix, the parallel subtask index of the sink
 * and a rolling counter. For example the file {@code "part-1-17"} contains the data from
 * {@code subtask 1} of the sink and is the {@code 17th} bucket created by that subtask. Per default
 * the part prefix is {@code "part"} but this can be configured using {@link #setPartPrefix(String)}.
 * When a part file becomes bigger than the user-specified batch size the current part file is closed,
 * the part counter is increased and a new part file is created. The batch size defaults to {@code 384MB},
 * this can be configured using {@link #setBatchSize(long)}.
 *
 *
 * <p>In some scenarios, the open buckets are required to change based on time. In these cases, the sink
 * needs to determine when a bucket has become inactive, in order to flush and close the part file.
 * To support this there are two configurable settings:
 * <ol>
 *     <li>the frequency to check for inactive buckets, configured by {@link #setInactiveBucketCheckInterval(long)},
 *     and</li>
 *     <li>the minimum amount of time a bucket has to not receive any data before it is considered inactive,
 *     configured by {@link #setInactiveBucketThreshold(long)}</li>
 * </ol>
 * Both of these parameters default to {@code 60, 000 ms}, or {@code 1 min}.
 *
 *
 * <p>Part files can be in one of three states: {@code in-progress}, {@code pending} or {@code finished}.
 * The reason for this is how the sink works together with the checkpointing mechanism to provide exactly-once
 * semantics and fault-tolerance. The part file that is currently being written to is {@code in-progress}. Once
 * a part file is closed for writing it becomes {@code pending}. When a checkpoint is successful the currently
 * pending files will be moved to {@code finished}.
 *
 *
 * <p>If case of a failure, and in order to guarantee exactly-once semantics, the sink should roll back to the state it
 * had when that last successful checkpoint occurred. To this end, when restoring, the restored files in {@code pending}
 * state are transferred into the {@code finished} state while any {@code in-progress} files are rolled back, so that
 * they do not contain data that arrived after the checkpoint from which we restore. If the {@code FileSystem} supports
 * the {@code truncate()} method this will be used to reset the file back to its previous state. If not, a special
 * file with the same name as the part file and the suffix {@code ".valid-length"} will be created that contains the
 * length up to which the file contains valid data. When reading the file, it must be ensured that it is only read up
 * to that point. The prefixes and suffixes for the different file states and valid-length files can be configured
 * using the adequate setter method, e.g.  #setPendingSuffix(String) TODO: fix javadoc.
 *
 *
 * <p><b>NOTE:</b>
 * <ol>
 *     <li>
 *         If checkpointing is not enabled the pending files will never be moved to the finished state. In that case,
 *         the pending suffix/prefix can be set to {@code ""} to make the sink work in a non-fault-tolerant way but
 *         still provide output without prefixes and suffixes.
 *     </li>
 *     <li>
 *         The part files are written using an instance of {@link Writer}. By default, a
 *         {@link StringWriter} is used, which writes the result of {@code toString()} for
 *         every element, separated by newlines. You can configure the writer using the
 *         {@link #setWriter(Writer)}. For example, {@link SequenceFileWriter}
 *         can be used to write Hadoop {@code SequenceFiles}.
 *     </li>
 * </ol>
 *
 *
 * <p>Example:
 * <pre>{@code
 *     new BucketingSink<Tuple2<IntWritable, Text>>(outPath)
 *         .setWriter(new SequenceFileWriter<IntWritable, Text>())
 *         .setBucketer(new DateTimeBucketer("yyyy-MM-dd--HHmm")
 * }</pre>
 *
 * <p>This will create a sink that writes to {@code SequenceFiles} and rolls every minute.
 *
 * @see DateTimeBucketer
 * @see StringWriter
 * @see SequenceFileWriter
 *
 * @param <T> Type of the elements emitted by this sink
 */
public class BucketingSink<T>
		extends RichSinkFunction<T>
		implements InputTypeConfigurable, CheckpointedFunction, CheckpointListener, ProcessingTimeCallback {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(BucketingSink.class);

	// --------------------------------------------------------------------------------------------
	//  User configuration values
	// --------------------------------------------------------------------------------------------
	// These are initialized with some defaults but are meant to be changeable by the user

	/**
	 * The default maximum size of part files (currently {@code 384 MB}).
	 */
	private static final long DEFAULT_BATCH_SIZE = 1024L * 1024L * 384L;

	/**
	 * The default time between checks for inactive buckets. By default, {60 sec}.
	 */
	private static final long DEFAULT_INACTIVE_BUCKET_CHECK_INTERVAL_MS = 60 * 1000L;

	/**
	 * The default threshold (in {@code ms}) for marking a bucket as inactive and
	 * closing its part files. By default, {60 sec}.
	 */
	private static final long DEFAULT_INACTIVE_BUCKET_THRESHOLD_MS = 60 * 1000L;

	/**
	 * The suffix for {@code in-progress} part files. These are files we are
	 * currently writing to, but which were not yet confirmed by a checkpoint.
	 */
	private static final String DEFAULT_IN_PROGRESS_SUFFIX = ".in-progress";

	/**
	 * The prefix for {@code in-progress} part files. These are files we are
	 * currently writing to, but which were not yet confirmed by a checkpoint.
	 */
	private static final String DEFAULT_IN_PROGRESS_PREFIX = "_";

	/**
	 * The suffix for {@code pending} part files. These are closed files that we are
	 * not currently writing to (inactive or reached {@link #batchSize}), but which
	 * were not yet confirmed by a checkpoint.
	 */
	private static final String DEFAULT_PENDING_SUFFIX = ".pending";

	/**
	 * The suffix for {@code final} part files. These are files that were
	 * confirmed by a checkpoint. Note that this field is only relevant if
	 * {@link #eventualConsistencySupport} is set to<code>true</code>.
	 */
	private static final String DEFAULT_FINAL_SUFFIX = ".final";

	/**
	 * The prefix for {@code pending} part files. These are closed files that we are
	 * not currently writing to (inactive or reached {@link #batchSize}), but which
	 * were not yet confirmed by a checkpoint.
	 */
	private static final String DEFAULT_PENDING_PREFIX = "_";

	/**
	 * When {@code truncate()} is not supported by the used {@link FileSystem}, we create
	 * a file along the part file with this suffix that contains the length up to which
	 * the part file is valid.
	 */
	private static final String DEFAULT_VALID_SUFFIX = ".valid-length";

	/**
	 * When {@code truncate()} is not supported by the used {@link FileSystem}, we create
	 * a file along the part file with this preffix that contains the length up to which
	 * the part file is valid.
	 */
	private static final String DEFAULT_VALID_PREFIX = "_";

	/**
	 * The default prefix for part files.
	 */
	private static final String DEFAULT_PART_REFIX = "part";

	/**
	 * The default timeout for asynchronous operations such as recoverLease and truncate (in {@code ms}).
	 */
	private static final long DEFAULT_ASYNC_TIMEOUT_MS = 60 * 1000;

	/**
	 * The base {@code Path} that stores all bucket directories.
	 */
	private final String basePath;

	/**
	 * The {@code Bucketer} that is used to determine the path of bucket directories.
	 */
	private Bucketer<T> bucketer;

	/**
	 * We have a template and call duplicate() for each parallel writer in open() to get the actual
	 * writer that is used for the part files.
	 */
	private Writer<T> writerTemplate;

	private long batchSize = DEFAULT_BATCH_SIZE;
	private long inactiveBucketCheckInterval = DEFAULT_INACTIVE_BUCKET_CHECK_INTERVAL_MS;
	private long inactiveBucketThreshold = DEFAULT_INACTIVE_BUCKET_THRESHOLD_MS;

	// These are the actually configured prefixes/suffixes
	private String inProgressSuffix = DEFAULT_IN_PROGRESS_SUFFIX;
	private String inProgressPrefix = DEFAULT_IN_PROGRESS_PREFIX;

	private String pendingSuffix = DEFAULT_PENDING_SUFFIX;
	private String pendingPrefix = DEFAULT_PENDING_PREFIX;

	private String finalSuffix = DEFAULT_FINAL_SUFFIX;

	private String validLengthSuffix = DEFAULT_VALID_SUFFIX;
	private String validLengthPrefix = DEFAULT_VALID_PREFIX;

	private String partPrefix = DEFAULT_PART_REFIX;

	/**
	 * The timeout for asynchronous operations such as recoverLease and truncate (in {@code ms}).
	 */
	private long asyncTimeout = DEFAULT_ASYNC_TIMEOUT_MS;

	private boolean eventualConsistencySupport;

	private transient PartFilePromotionStrategy partFilePromotionStrategy;

	// --------------------------------------------------------------------------------------------
	//  Internal fields (not configurable by user)
	// --------------------------------------------------------------------------------------------

	/**
	 * The state object that is handled by Flink from snapshot/restore. This contains state for
	 * every open bucket: the current in-progress part file path, its valid length and the pending part files.
	 */
	private transient State<T> state;

	private transient ListState<State<T>> restoredBucketStates;

	/**
	 * The FileSystem reference.
	 */
	private transient FileSystem fs;

	private transient Clock clock;

	private transient ProcessingTimeService processingTimeService;

	/**
	 * Creates a new {@code BucketingSink} that writes files to the given base directory.
	 *
	 *
	 * <p>This uses a{@link DateTimeBucketer} as {@link Bucketer} and a {@link StringWriter} has writer.
	 * The maximum bucket size is set to 384 MB.
	 *
	 * @param basePath The directory to which to write the bucket files.
	 */
	public BucketingSink(String basePath) {
		this.basePath = basePath;
		this.bucketer = new DateTimeBucketer<>();
		this.writerTemplate = new StringWriter<>();
	}

	@Override
	@SuppressWarnings("unchecked")
	public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
		if (this.writerTemplate instanceof InputTypeConfigurable) {
			((InputTypeConfigurable) writerTemplate).setInputType(type, executionConfig);
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		Preconditions.checkState(this.restoredBucketStates == null, "The operator has already been initialized.");

		initiPartFilePromotionStrategy();

		try {
			initFileSystem();
		} catch (IOException e) {
			throw new RuntimeException("Error while creating FileSystem when initializing the state of the BucketingSink.", e);
		}

		restoredBucketStates = context.getOperatorStateStore().getListState(
			new ListStateDescriptor<>("bucket-states", TypeInformation.of(new TypeHint<State<T>>() {})));

		int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
		if (context.isRestored()) {
			LOG.info("Restoring state for the {} (taskIdx={}).", getClass().getSimpleName(), subtaskIndex);

			for (State<T> recoveredState : restoredBucketStates.get()) {
				handleRestoredBucketState(recoveredState);
				if (LOG.isDebugEnabled()) {
					LOG.debug("{} idx {} restored {}", getClass().getSimpleName(), subtaskIndex, recoveredState);
				}
			}
		} else {
			LOG.info("No state to restore for the {} (taskIdx={}).", getClass().getSimpleName(), subtaskIndex);
		}
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		state = new State<>();

		processingTimeService =
				((StreamingRuntimeContext) getRuntimeContext()).getProcessingTimeService();

		long currentProcessingTime = processingTimeService.getCurrentProcessingTime();

		processingTimeService.registerTimer(currentProcessingTime + inactiveBucketCheckInterval, this);

		this.clock = new Clock() {
			@Override
			public long currentTimeMillis() {
				return processingTimeService.getCurrentProcessingTime();
			}
		};
	}

	private void initFileSystem() throws IOException {
		if (fs == null) {
			fs = FileSystem.get(new Path(basePath).toUri());
		}
	}

	private void initiPartFilePromotionStrategy() {
		if (eventualConsistencySupport) {
			partFilePromotionStrategy =
				new EventuallyConsistentFileSystemPartFilePromotionStrategy(
					pendingSuffix,
					finalSuffix);
		} else {
			partFilePromotionStrategy =
				new ConsistentFileSystemPartFilePromotionStrategy(
					inProgressSuffix,
					inProgressPrefix,
					pendingSuffix,
					pendingPrefix);
		}
	}

	@Override
	public void close() throws Exception {
		if (state != null) {
			for (Map.Entry<String, BucketState<T>> entry : state.bucketStates.entrySet()) {
				closeCurrentPartFile(entry.getValue());
			}
		}
	}

	@Override
	public void invoke(T value, Context context) throws Exception {
		Path bucketPath = bucketer.getBucketPath(clock, new Path(basePath), value);

		long currentProcessingTime = processingTimeService.getCurrentProcessingTime();

		BucketState<T> bucketState = state.getBucketState(bucketPath);
		if (bucketState == null) {
			bucketState = new BucketState<>(currentProcessingTime);
			state.addBucketState(bucketPath, bucketState);
		}

		if (shouldRoll(bucketState)) {
			openNewPartFile(bucketPath, bucketState);
		}

		bucketState.writer.write(value);
		bucketState.lastWrittenToTime = currentProcessingTime;
	}

	/**
	 * Returns {@code true} if the current {@code part-file} should be closed and a new should be created.
	 * This happens if:
	 * <ol>
	 *     <li>no file is created yet for the task to write to, or</li>
	 *     <li>the current file has reached the maximum bucket size.</li>
	 * </ol>
	 */
	private boolean shouldRoll(BucketState<T> bucketState) throws IOException {
		boolean shouldRoll = false;
		int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
		if (!bucketState.isWriterOpen) {
			shouldRoll = true;
			LOG.debug("BucketingSink {} starting new bucket.", subtaskIndex);
		} else {
			long writePosition = bucketState.writer.getPos();
			if (writePosition > batchSize) {
				shouldRoll = true;
				LOG.debug(
					"BucketingSink {} starting new bucket because file position {} is above batch size {}.",
					subtaskIndex,
					writePosition,
					batchSize);
			}
		}
		return shouldRoll;
	}

	@Override
	public void onProcessingTime(long timestamp) throws Exception {
		long currentProcessingTime = processingTimeService.getCurrentProcessingTime();

		checkForInactiveBuckets(currentProcessingTime);

		processingTimeService.registerTimer(currentProcessingTime + inactiveBucketCheckInterval, this);
	}

	/**
	 * Checks for inactive buckets, and closes them. Buckets are considered inactive if they have not been
	 * written to for a period greater than {@code inactiveBucketThreshold} ms. This enables in-progress
	 * files to be moved to the pending state and be finalised on the next checkpoint.
	 */
	private void checkForInactiveBuckets(long currentProcessingTime) throws Exception {

		synchronized (state.bucketStates) {
			for (Map.Entry<String, BucketState<T>> entry : state.bucketStates.entrySet()) {
				if (entry.getValue().lastWrittenToTime < currentProcessingTime - inactiveBucketThreshold) {
					LOG.debug("BucketingSink {} closing bucket due to inactivity of over {} ms.",
						getRuntimeContext().getIndexOfThisSubtask(), inactiveBucketThreshold);
					closeCurrentPartFile(entry.getValue());
				}
			}
		}
	}

	/**
	 * Closes the current part file and opens a new one with a new bucket path, as returned by the
	 * {@link Bucketer}. If the bucket is not new, then this will create a new file with the same path
	 * as its predecessor, but with an increased rolling counter (see {@link BucketingSink}.
	 */
	private void openNewPartFile(Path bucketPath, BucketState<T> bucketState) throws Exception {
		closeCurrentPartFile(bucketState);

		try {
			if (fs.mkdirs(bucketPath)) {
				LOG.debug("Created new bucket directory: {}", bucketPath);
			}
		} catch (IOException e) {
			throw new RuntimeException("Could not create new bucket path.", e);
		}

		// The following loop tries different partCounter values in ascending order until it reaches the minimum
		// that is not yet used. This works since there is only one parallel subtask that tries names with this
		// subtask id. Otherwise we would run into concurrency issues here. This is aligned with the way we now
		// clean the base directory in case of rescaling.

		int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
		Path partPath = new Path(bucketPath, partPrefix + "-" + subtaskIndex + "-" + partFilePromotionStrategy.getPartFileSuffix(bucketState.partCounter++));
		while (fs.exists(partPath) ||
				fs.exists(partFilePromotionStrategy.getPendingPathFor(partPath)) ||
				fs.exists(partFilePromotionStrategy.getInProgressPathFor(partPath))) {
			partPath = new Path(bucketPath, partPrefix + "-" + subtaskIndex + "-" + partFilePromotionStrategy.getPartFileSuffix(bucketState.partCounter++));
		}

		LOG.debug("Next part path is {}", partPath.toString());
		bucketState.currentFile = partPath.toString();

		if (bucketState.writer == null) {
			bucketState.writer = writerTemplate.duplicate();
		}

		Path inProgressPath = partFilePromotionStrategy.getInProgressPathFor(partPath);
		bucketState.writer.open(fs, inProgressPath);
		bucketState.isWriterOpen = true;
	}

	/**
	 * Closes the current part file and moves it from the in-progress state to the pending state.
	 */
	private void closeCurrentPartFile(BucketState<T> bucketState) throws Exception {
		if (bucketState.isWriterOpen) {
			bucketState.writer.close();
			bucketState.isWriterOpen = false;
		}

		if (bucketState.currentFile != null) {
			Path currentPartPath = new Path(bucketState.currentFile);
			partFilePromotionStrategy.promoteToPending(fs, currentPartPath);
			LOG.debug("Promoted file {} to pending state.", currentPartPath);
			bucketState.pendingFiles.add(currentPartPath.toString());
			bucketState.currentFile = null;
		}
	}

	private Path getValidLengthPathFor(Path path) {
		return new Path(path.getParent(), validLengthPrefix + path.getName()).suffix(validLengthSuffix);
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		synchronized (state.bucketStates) {

			Iterator<Map.Entry<String, BucketState<T>>> bucketStatesIt = state.bucketStates.entrySet().iterator();
			while (bucketStatesIt.hasNext()) {
				BucketState<T> bucketState = bucketStatesIt.next().getValue();
				synchronized (bucketState.pendingFilesPerCheckpoint) {

					Iterator<Map.Entry<Long, List<String>>> pendingCheckpointsIt =
						bucketState.pendingFilesPerCheckpoint.entrySet().iterator();

					while (pendingCheckpointsIt.hasNext()) {

						Map.Entry<Long, List<String>> entry = pendingCheckpointsIt.next();
						Long pastCheckpointId = entry.getKey();
						List<String> pendingPaths = entry.getValue();

						if (pastCheckpointId <= checkpointId) {
							LOG.debug("Promote pending files to final state for checkpoint {}", pastCheckpointId);

							for (String filename : pendingPaths) {
								Path finalPath = new Path(filename);
								partFilePromotionStrategy.promoteToFinal(fs, finalPath);
								LOG.debug(
									"Promote file {} from pending to final state after having completed checkpoint {}.",
									finalPath,
									pastCheckpointId);
							}
							pendingCheckpointsIt.remove();
						}
					}

					if (!bucketState.isWriterOpen &&
						bucketState.pendingFiles.isEmpty() &&
						bucketState.pendingFilesPerCheckpoint.isEmpty()) {

						// We've dealt with all the pending files and the writer for this bucket is not currently open.
						// Therefore this bucket is currently inactive and we can remove it from our state.
						bucketStatesIt.remove();
					}
				}
			}
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		Preconditions.checkNotNull(restoredBucketStates, "The operator has not been properly initialized.");

		restoredBucketStates.clear();

		synchronized (state.bucketStates) {
			int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();

			for (Map.Entry<String, BucketState<T>> bucketStateEntry : state.bucketStates.entrySet()) {
				BucketState<T> bucketState = bucketStateEntry.getValue();

				if (bucketState.isWriterOpen) {
					bucketState.currentFileValidLength = bucketState.writer.flush();
				}

				synchronized (bucketState.pendingFilesPerCheckpoint) {
					bucketState.pendingFilesPerCheckpoint.put(context.getCheckpointId(), bucketState.pendingFiles);
				}
				bucketState.pendingFiles = new ArrayList<>();
			}
			restoredBucketStates.add(state);

			if (LOG.isDebugEnabled()) {
				LOG.debug("{} idx {} checkpointed {}.", getClass().getSimpleName(), subtaskIdx, state);
			}
		}
	}

	private void handleRestoredBucketState(State<T> restoredState) {
		Preconditions.checkNotNull(restoredState);

		for (BucketState<T> bucketState : restoredState.bucketStates.values()) {

			// we can clean all the pending files since they were renamed to
			// final files after this checkpoint was successful
			// (we re-start from the last **successful** checkpoint)
			bucketState.pendingFiles.clear();

			handlePendingInProgressFile(bucketState.currentFile, bucketState.currentFileValidLength);

			// Now that we've restored the bucket to a valid state, reset the current file info
			bucketState.currentFile = null;
			bucketState.currentFileValidLength = -1;
			bucketState.isWriterOpen = false;

			handlePendingFilesForPreviousCheckpoints(bucketState.pendingFilesPerCheckpoint);

			bucketState.pendingFilesPerCheckpoint.clear();
		}
	}

	private void handlePendingInProgressFile(String file, long validLength) {
		if (file != null) {

			// We were writing to a file when the last checkpoint occurred. This file can either
			// be still in-progress or became a pending file at some point after the checkpoint.
			// Either way, we have to truncate it back to a valid state (or write a .valid-length
			// file that specifies up to which length it is valid) and rename it to the final name
			// before starting a new bucket file.

			final Path partPath = new Path(file);
			try {
				partFilePromotionStrategy.promoteToFinal(fs, partPath);

				// truncate it or write a ".valid-length" file to specify up to which point it is valid
				if (fs.isTruncateSupported() && !eventualConsistencySupport) {
					LOG.debug("Truncating {} to valid length {}", partPath, validLength);

					// some-one else might still hold the lease from a previous try, we are
					// recovering, after all ...
					if (fs.isDistributedFS()) {
						try {
							fs.recoverLease(partPath);
						} catch (final IOException e) {
							LOG.warn("RecoverLease failed due to: {}", e);
						}
					}

					boolean asyncTruncate = false;
					final StopWatch truncateStopWatch = new StopWatch();
					truncateStopWatch.start();
					while (true) {
						try {
							asyncTruncate = fs.truncate(partPath, validLength);
							break;
						} catch (final IOException e) {
							if (truncateStopWatch.getTime() > asyncTimeout) {
								LOG.error("Truncating file {} failed.", partPath, e);
							} else {
								LOG.warn("Truncating file {} failed due to: {}. Retrying...", partPath, e.getMessage());
								try {
									Thread.sleep(500);
								} catch (final InterruptedException ie) {
									Thread.currentThread().interrupt();
									break;
								}
							}
						}
					}

					long newLen = fs.getFileStatus(partPath).getLen();
					if (asyncTruncate) { //TODO: check if fs is eventually consistent
						// we must wait for the asynchronous truncate operation to complete
						final long startTime = System.currentTimeMillis();
						while (newLen != validLength) {
							if (System.currentTimeMillis() - startTime > asyncTimeout) {
								break;
							}
							try {
								Thread.sleep(500);
							} catch (final InterruptedException e) {
								Thread.currentThread().interrupt();
								break;
							}
							newLen = fs.getFileStatus(partPath).getLen();
						}
					}

					if (newLen != validLength) {
						throw new RuntimeException("Truncate did not truncate to right length. Should be " + validLength + " is " + newLen + ".");
					}
				} else {
					LOG.debug("Writing valid-length file for {} to specify valid length {}", partPath, validLength);
					Path validLengthFilePath = getValidLengthPathFor(partPath);
					if (!fs.exists(validLengthFilePath) && fs.exists(partPath)) {
						try (FSDataOutputStream lengthFileOut = fs.create(validLengthFilePath, FileSystem.WriteMode.NO_OVERWRITE)) {
							lengthFileOut.write(Long.toString(validLength).getBytes(StandardCharsets.UTF_8));
						}
					}
				}

			} catch (final Exception e) {
				throw new RuntimeException("Error while restoring BucketingSink state.", e);
			}
		}
	}

	private void handlePendingFilesForPreviousCheckpoints(Map<Long, List<String>> pendingFilesPerCheckpoint) {
		// Move files that are confirmed by a checkpoint but did not get moved to final location
		// because the checkpoint notification did not happen before a failure

		LOG.debug("Promoting pending files to final state on restore.");

		Set<Long> pastCheckpointIds = pendingFilesPerCheckpoint.keySet();
		for (Long pastCheckpointId : pastCheckpointIds) {
			// All the pending files are buckets that have been completed but are waiting to be renamed
			// to their final name
			for (String filename : pendingFilesPerCheckpoint.get(pastCheckpointId)) {
				Path finalPath = new Path(filename); //TODO: rename
				try {
					partFilePromotionStrategy.promoteToFinal(fs, finalPath);
					LOG.debug("Restoring BucketingSink State: Promoting file {} from pending to final state after complete checkpoint {}.", finalPath, pastCheckpointId);
				} catch (Exception e) {
					throw new RuntimeException("Restoring BucketingSink State: Error while promoting pending file "
						+ finalPath + " to final state: {}", e);
				}
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Setters for User configuration values
	// --------------------------------------------------------------------------------------------

	/**
	 * Sets the maximum bucket size in bytes.
	 *
	 *
	 * <p>When a bucket part file becomes larger than this size a new bucket part file is started and
	 * the old one is closed. The name of the bucket files depends on the {@link Bucketer}.
	 *
	 * @param batchSize The bucket part file size in bytes.
	 */
	public BucketingSink<T> setBatchSize(long batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	/**
	 * Sets the default time between checks for inactive buckets.
	 *
	 * @param interval The timeout, in milliseconds.
	 */
	public BucketingSink<T> setInactiveBucketCheckInterval(long interval) {
		this.inactiveBucketCheckInterval = interval;
		return this;
	}

	/**
	 * Sets the default threshold for marking a bucket as inactive and closing its part files.
	 * Buckets which haven't been written to for at least this period of time become inactive.
	 *
	 * @param threshold The timeout, in milliseconds.
	 */
	public BucketingSink<T> setInactiveBucketThreshold(long threshold) {
		this.inactiveBucketThreshold = threshold;
		return this;
	}

	/**
	 * Sets the {@link Bucketer} to use for determining the bucket files to write to.
	 *
	 * @param bucketer The bucketer to use.
	 */
	public BucketingSink<T> setBucketer(Bucketer<T> bucketer) {
		this.bucketer = bucketer;
		return this;
	}

	/**
	 * Sets the {@link Writer} to be used for writing the incoming elements to bucket files.
	 *
	 * @param writer The {@code Writer} to use.
	 */
	public BucketingSink<T> setWriter(Writer<T> writer) {
		this.writerTemplate = writer;
		return this;
	}

	/**
	 * Sets the suffix of in-progress part files. The default is {@code "in-progress"}.
	 */
	public BucketingSink<T> setInProgressSuffix(String inProgressSuffix) {
		this.inProgressSuffix = inProgressSuffix;
		return this;
	}

	/**
	 * Sets the prefix of in-progress part files. The default is {@code "_"}.
	 */
	public BucketingSink<T> setInProgressPrefix(String inProgressPrefix) {
		this.inProgressPrefix = inProgressPrefix;
		return this;
	}

	/**
	 * Sets the suffix of pending part files. The default is {@code ".pending"}.
	 */
	public BucketingSink<T> setPendingSuffix(String pendingSuffix) {
		this.pendingSuffix = pendingSuffix;
		return this;
	}

	/**
	 * Sets the prefix of pending part files. The default is {@code "_"}.
	 */
	public BucketingSink<T> setPendingPrefix(String pendingPrefix) {
		this.pendingPrefix = pendingPrefix;
		return this;
	}

	/**
	 * Sets the suffix of final part files. The default is {@code ".final"}.
	 */
	public BucketingSink<T> setFinalSuffix(String pendingSuffix) {
		this.finalSuffix = finalSuffix;
		return this;
	}

	/**
	 * Sets the suffix of valid-length files. The default is {@code ".valid-length"}.
	 */
	public BucketingSink<T> setValidLengthSuffix(String validLengthSuffix) {
		this.validLengthSuffix = validLengthSuffix;
		return this;
	}

	/**
	 * Sets the prefix of valid-length files. The default is {@code "_"}.
	 */
	public BucketingSink<T> setValidLengthPrefix(String validLengthPrefix) {
		this.validLengthPrefix = validLengthPrefix;
		return this;
	}

	/**
	 * Sets the prefix of part files.  The default is {@code "part"}.
	 */
	public BucketingSink<T> setPartPrefix(String partPrefix) {
		this.partPrefix = partPrefix;
		return this;
	}

	/**
	 * Disable cleanup of leftover in-progress/pending files when the sink is opened.
	 *
	 *
	 * <p>This should only be disabled if using the sink without checkpoints, to not remove
	 * the files already in the directory.
	 *
	 * @deprecated This option is deprecated and remains only for backwards compatibility.
	 * We do not clean up lingering files anymore.
	 */
	@Deprecated
	public BucketingSink<T> disableCleanupOnOpen() {
		return this;
	}

	/**
	 * Sets the default timeout for asynchronous operations such as recoverLease and truncate.
	 *
	 * @param timeout The timeout, in milliseconds.
	 */
	public BucketingSink<T> setAsyncTimeout(long timeout) {
		this.asyncTimeout = timeout;
		return this;
	}

	/**
	 * Enables the Sink's mode to be used with eventually consistent file systems.
	 */
	public void enableEventualConsistencySupport() {
		eventualConsistencySupport = true;
	}

	@VisibleForTesting
	public State<T> getState() {
		return state;
	}

	// --------------------------------------------------------------------------------------------
	//  Internal Classes
	// --------------------------------------------------------------------------------------------

	/**
	 * This is used during snapshot/restore to keep track of in-progress buckets.
	 * For each bucket, we maintain a state.
	 */
	static final class State<T> {

		/**
		 * For every bucket directory (key), we maintain a bucket state (value).
		 */
		final Map<String, BucketState<T>> bucketStates = new HashMap<>();

		void addBucketState(Path bucketPath, BucketState<T> state) {
			synchronized (bucketStates) {
				bucketStates.put(bucketPath.toString(), state);
			}
		}

		BucketState<T> getBucketState(Path bucketPath) {
			synchronized (bucketStates) {
				return bucketStates.get(bucketPath.toString());
			}
		}

		@Override
		public String toString() {
			return bucketStates.toString();
		}
	}

	/**
	 * This is used for keeping track of the current in-progress buckets and files that we mark
	 * for moving from pending to final location after we get a checkpoint-complete notification.
	 */
	static final class BucketState<T> {

		/**
		 * The file that was in-progress when the last checkpoint occurred.
		 */
		String currentFile;

		/**
		 * The valid length of the in-progress file at the time of the last checkpoint.
		 */
		long currentFileValidLength = -1;

		/**
		 * The time this bucket was last written to.
		 */
		long lastWrittenToTime;

		/**
		 * Pending files that accumulated since the last checkpoint.
		 */
		List<String> pendingFiles = new ArrayList<>();

		/**
		 * When doing a checkpoint we move the pending files since the last checkpoint to this map
		 * with the id of the checkpoint. When we get the checkpoint-complete notification we move
		 * pending files of completed checkpoints to their final location.
		 */
		final Map<Long, List<String>> pendingFilesPerCheckpoint = new HashMap<>();

		/**
		 * For counting the part files inside a bucket directory. Part files follow the pattern
		 * {@code "{part-prefix}-{subtask}-{count}"}. When creating new part files we increase the counter.
		 */
		private transient int partCounter;

		/**
		 * Tracks if the writer is currently opened or closed.
		 */
		private transient boolean isWriterOpen;

		/**
		 * The actual writer that we user for writing the part files.
		 */
		private transient Writer<T> writer;

		@Override
		public String toString() {
			return
				"In-progress=" + currentFile +
					" validLength=" + currentFileValidLength +
					" pendingForNextCheckpoint=" + pendingFiles +
					" pendingForPrevCheckpoints=" + pendingFilesPerCheckpoint +
					" lastModified@" + lastWrittenToTime;
		}

		BucketState(long lastWrittenToTime) {
			this.lastWrittenToTime = lastWrittenToTime;
		}
	}

}
