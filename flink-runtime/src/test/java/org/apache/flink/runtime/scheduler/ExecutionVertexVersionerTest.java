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

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link ExecutionVertexVersioner}.
 */
public class ExecutionVertexVersionerTest extends TestLogger {

	private static final ExecutionVertexID TEST_EXECUTION_VERTEX_ID = new ExecutionVertexID(new JobVertexID(), 0);

	private ExecutionVertexVersioner executionVertexVersioner;

	@Before
	public void setUp() {
		executionVertexVersioner = new ExecutionVertexVersioner();
	}

	@Test
	public void vertexModifiedOnce() {
		final ExecutionVertexVersion executionVertexVersion = executionVertexVersioner.recordModification(TEST_EXECUTION_VERTEX_ID);
		assertFalse(executionVertexVersioner.isModified(executionVertexVersion));
	}

	@Test
	public void vertexModifiedTwice() {
		final ExecutionVertexVersion executionVertexVersion = executionVertexVersioner.recordModification(TEST_EXECUTION_VERTEX_ID);
		executionVertexVersioner.recordModification(TEST_EXECUTION_VERTEX_ID);
		assertTrue(executionVertexVersioner.isModified(executionVertexVersion));
	}

	@Test
	public void throwsExceptionIfVertexWasNeverModified() {
		try {
			executionVertexVersioner.isModified(new ExecutionVertexVersion(TEST_EXECUTION_VERTEX_ID, 0));
			fail("Expected exception not thrown");
		} catch (final IllegalStateException e) {
			assertThat(e.getMessage(), containsString("Execution vertex " + TEST_EXECUTION_VERTEX_ID + " does not have a recorded version"));
		}
	}
}
