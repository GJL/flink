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

package org.apache.flink.runtime.fs.hdfs;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.ExceptionUtils;

import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link FileSystem} that wraps an {@link org.apache.hadoop.fs.FileSystem Hadoop File System}.
 */
public class HadoopFileSystem extends FileSystem {

	private static final Logger LOG = LoggerFactory.getLogger(HadoopFileSystem.class);

	/**
	 * The wrapped Hadoop File System.
	 */
	private final org.apache.hadoop.fs.FileSystem fs;

	private transient Method refTruncate;

	private transient Method refRecoverLease;

	/**
	 * Wraps the given Hadoop File System object as a Flink File System object.
	 * The given Hadoop file system object is expected to be initialized already.
	 *
	 * @param hadoopFileSystem The Hadoop FileSystem that will be used under the hood.
	 */
	public HadoopFileSystem(org.apache.hadoop.fs.FileSystem hadoopFileSystem) {
		this.fs = checkNotNull(hadoopFileSystem, "hadoopFileSystem");
	}

	/**
	 * Gets the underlying Hadoop FileSystem.
	 *
	 * @return The underlying Hadoop FileSystem.
	 */
	public org.apache.hadoop.fs.FileSystem getHadoopFileSystem() {
		return this.fs;
	}

	// ------------------------------------------------------------------------
	//  file system methods
	// ------------------------------------------------------------------------

	@Override
	public Path getWorkingDirectory() {
		return new Path(this.fs.getWorkingDirectory().toUri());
	}

	public Path getHomeDirectory() {
		return new Path(this.fs.getHomeDirectory().toUri());
	}

	@Override
	public URI getUri() {
		return fs.getUri();
	}

	@Override
	public FileStatus getFileStatus(final Path f) throws IOException {
		org.apache.hadoop.fs.FileStatus status = this.fs.getFileStatus(new org.apache.hadoop.fs.Path(f.toString()));
		return new HadoopFileStatus(status);
	}

	@Override
	public BlockLocation[] getFileBlockLocations(final FileStatus file, final long start, final long len)
		throws IOException {
		if (!(file instanceof HadoopFileStatus)) {
			throw new IOException("file is not an instance of DistributedFileStatus");
		}

		final HadoopFileStatus f = (HadoopFileStatus) file;

		final org.apache.hadoop.fs.BlockLocation[] blkLocations = fs.getFileBlockLocations(f.getInternalFileStatus(),
			start, len);

		// Wrap up HDFS specific block location objects
		final HadoopBlockLocation[] distBlkLocations = new HadoopBlockLocation[blkLocations.length];
		for (int i = 0; i < distBlkLocations.length; i++) {
			distBlkLocations[i] = new HadoopBlockLocation(blkLocations[i]);
		}

		return distBlkLocations;
	}

	@Override
	public HadoopDataInputStream open(final Path f, final int bufferSize) throws IOException {
		final org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(f.toString());
		final org.apache.hadoop.fs.FSDataInputStream fdis = this.fs.open(path, bufferSize);
		return new HadoopDataInputStream(fdis);
	}

	@Override
	public HadoopDataInputStream open(final Path f) throws IOException {
		final org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(f.toString());
		final org.apache.hadoop.fs.FSDataInputStream fdis = fs.open(path);
		return new HadoopDataInputStream(fdis);
	}

	@Override
	@SuppressWarnings("deprecation")
	public HadoopDataOutputStream create(final Path f, final boolean overwrite, final int bufferSize,
										 final short replication, final long blockSize) throws IOException {
		final org.apache.hadoop.fs.FSDataOutputStream fdos = this.fs.create(
			new org.apache.hadoop.fs.Path(f.toString()), overwrite, bufferSize, replication, blockSize);
		return new HadoopDataOutputStream(fdos);
	}

	@Override
	public HadoopDataOutputStream create(final Path f, final WriteMode overwrite) throws IOException {
		final org.apache.hadoop.fs.FSDataOutputStream fsDataOutputStream = this.fs
			.create(new org.apache.hadoop.fs.Path(f.toString()), overwrite == WriteMode.OVERWRITE);
		return new HadoopDataOutputStream(fsDataOutputStream);
	}

	@Override
	public boolean delete(final Path f, final boolean recursive) throws IOException {
		return this.fs.delete(new org.apache.hadoop.fs.Path(f.toString()), recursive);
	}

	@Override
	public FileStatus[] listStatus(final Path f) throws IOException {
		final org.apache.hadoop.fs.FileStatus[] hadoopFiles = this.fs.listStatus(new org.apache.hadoop.fs.Path(f.toString()));
		final FileStatus[] files = new FileStatus[hadoopFiles.length];

		// Convert types
		for (int i = 0; i < files.length; i++) {
			files[i] = new HadoopFileStatus(hadoopFiles[i]);
		}

		return files;
	}

	@Override
	public boolean mkdirs(final Path f) throws IOException {
		return this.fs.mkdirs(new org.apache.hadoop.fs.Path(f.toString()));
	}

	@Override
	public boolean rename(final Path src, final Path dst) throws IOException {
		return this.fs.rename(new org.apache.hadoop.fs.Path(src.toString()),
			new org.apache.hadoop.fs.Path(dst.toString()));
	}

	@SuppressWarnings("deprecation")
	@Override
	public long getDefaultBlockSize() {
		return this.fs.getDefaultBlockSize();
	}

	@Override
	public boolean isDistributedFS() {
		return true;
	}

	@Override
	public boolean truncate(final Path f, final long newLength) throws IOException {
		if (refTruncate == null) {
			refTruncate = reflectTruncate(fs);
		}

		if (refTruncate != null) {
			try {
				return (boolean) refTruncate.invoke(fs, new org.apache.hadoop.fs.Path(f.toString()), newLength);
			} catch (final IllegalAccessException e) {
				throw new RuntimeException("Could not invoke truncate.", e);
			} catch (final InvocationTargetException e) {
				ExceptionUtils.rethrowIOException(e.getCause());
			}
		}

		throw new UnsupportedOperationException("Hadoop " +
			VersionInfo.getVersion() + " does not support truncate.");
	}

	@Override
	public boolean isTruncateSupported() {
		if (refTruncate == null) {
			refTruncate = reflectTruncate(fs);
		}

		return refTruncate != null;
	}

	@Override
	public boolean recoverLease(final Path f) throws IOException {
		if (refRecoverLease == null) {
			refRecoverLease = getMethodByName(fs, "recoverLease", org.apache.hadoop.fs.Path.class);
		}

		if (refRecoverLease != null) {
			try {
				return (boolean) refRecoverLease.invoke(fs, new org.apache.hadoop.fs.Path(f.toString()));
			} catch (IllegalAccessException | ClassCastException e) {
				LOG.debug("Method recoverLease is not supported by file system {}.",
					fs.getClass().getSimpleName(), e);
			} catch (final InvocationTargetException e) {
				ExceptionUtils.rethrowIOException(e.getCause());
			}
		}

		return super.recoverLease(f);
	}


	/**
	 * Gets the truncate() call using reflection.
	 */
	@Nullable
	private Method reflectTruncate(final org.apache.hadoop.fs.FileSystem fs) {
		Method m = getMethodByName(fs, "truncate", org.apache.hadoop.fs.Path.class, long.class);

		if (m == null) {
			return null;
		}

		// verify that truncate actually works
		final Path testPath = new Path(UUID.randomUUID().toString());
		try {
			final boolean asyncTruncate = (boolean) m.invoke(fs, new org.apache.hadoop.fs.Path(testPath.toString()), 2);
		} catch (IllegalAccessException | ClassCastException e) {
			// Method is not accessible from this class or the return type was not a boolean.
			LOG.debug("Truncate is not supported.", e);
			m = null;
		} catch (final InvocationTargetException e) {
			// If truncate() can be invoked and is supported by the file system implementation,
			// an IOException should be thrown because the file name is randomly generated and
			// should not exist. We could create a file with a random name and truncate the file.
			// However we might not have permissions to do so. If the file system does not support
			// truncate(), it will throw UnsupportedMethodOperation.
			if (e.getCause() instanceof IOException) {
				LOG.debug("Expected exception when invoking truncate() on file {} as the file should not exist",
					testPath, e.getCause());
			} else {
				LOG.debug("Truncate is not supported.", e);
				m = null;
			}
		}

		return m;
	}

	@Nullable
	@VisibleForTesting
	private Method getMethodByName(final Object object, final String methodName, final Class<?>... parameterTypes) {
		try {
			return object.getClass().getMethod(methodName, parameterTypes);
		} catch (final NoSuchMethodException ex) {
			LOG.debug("{} not found.", methodName);
			return null;
		}
	}

}
