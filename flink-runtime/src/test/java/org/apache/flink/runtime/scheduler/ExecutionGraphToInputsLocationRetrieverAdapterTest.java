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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link ExecutionGraphToInputsLocationRetrieverAdapter}.
 */
public class ExecutionGraphToInputsLocationRetrieverAdapterTest extends TestLogger {

	/**
	 * Tests that can get the producers of consumed result partitions.
	 */
	@Test
	public void testGetConsumedResultPartitionsProducers() throws Exception {
		final JobVertex producer1 = ExecutionGraphTestUtils.createNoOpVertex(1);
		final JobVertex producer2 = ExecutionGraphTestUtils.createNoOpVertex(1);
		final JobVertex consumer = ExecutionGraphTestUtils.createNoOpVertex(1);
		consumer.connectNewDataSetAsInput(producer1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		consumer.connectNewDataSetAsInput(producer2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		final ExecutionGraph eg = ExecutionGraphTestUtils.createSimpleTestGraph(new JobID(), producer1, producer2, consumer);
		final ExecutionGraphToInputsLocationRetrieverAdapter inputsLocationsRetriever = new ExecutionGraphToInputsLocationRetrieverAdapter(eg);

		ExecutionVertexID evIdOfProducer1 = new ExecutionVertexID(producer1.getID(), 0);
		ExecutionVertexID evIdOfProducer2 = new ExecutionVertexID(producer2.getID(), 0);
		ExecutionVertexID evIdOfConsumer = new ExecutionVertexID(consumer.getID(), 0);

		Collection<Collection<ExecutionVertexID>> producersOfConsumer =
				inputsLocationsRetriever.getConsumedResultPartitionsProducers(evIdOfConsumer);

		assertThat(producersOfConsumer, Matchers.contains(Arrays.asList(evIdOfProducer1), Arrays.asList(evIdOfProducer2)));
	}

	/**
	 * Tests that when execution is not scheduled, getting task manager location will return null.
	 */
	@Test
	public void testGetNullTaskManagerLocationIfNotScheduled() throws Exception {
		final JobVertex jobVertex = ExecutionGraphTestUtils.createNoOpVertex(1);

		final ExecutionGraph eg = ExecutionGraphTestUtils.createSimpleTestGraph(new JobID(), jobVertex);
		final ExecutionGraphToInputsLocationRetrieverAdapter inputsLocationsRetriever = new ExecutionGraphToInputsLocationRetrieverAdapter(eg);

		ExecutionVertexID executionVertexId = new ExecutionVertexID(jobVertex.getID(), 0);
		Optional<CompletableFuture<TaskManagerLocation>> taskManagerLocation =
				inputsLocationsRetriever.getTaskManagerLocation(executionVertexId);

		assertFalse(taskManagerLocation.isPresent());
	}

	/**
	 * Tests that it can get the task manager location in an Execution.
	 */
	@Test
	public void testGetTaskManagerLocationWhenScheduled() throws Exception {
		final JobVertex jobVertex = ExecutionGraphTestUtils.createNoOpVertex(1);

		final TestingLogicalSlot testingLogicalSlot = new TestingLogicalSlot();
		final ExecutionGraph eg = ExecutionGraphTestUtils.createSimpleTestGraph(new JobID(), jobVertex);
		final ExecutionGraphToInputsLocationRetrieverAdapter inputsLocationsRetriever = new ExecutionGraphToInputsLocationRetrieverAdapter(eg);

		final ExecutionVertex onlyExecutionVertex = eg.getAllExecutionVertices().iterator().next();
		onlyExecutionVertex.deployToSlot(testingLogicalSlot);

		ExecutionVertexID consumerVertexId = new ExecutionVertexID(jobVertex.getID(), 0);
		Optional<CompletableFuture<TaskManagerLocation>> taskManagerLocationOptional = inputsLocationsRetriever.getTaskManagerLocation(consumerVertexId);

		assertTrue(taskManagerLocationOptional.isPresent());

		final CompletableFuture<TaskManagerLocation> taskManagerLocationFuture = taskManagerLocationOptional.get();
		assertEquals(testingLogicalSlot.getTaskManagerLocation(), taskManagerLocationFuture.get());
	}

}
