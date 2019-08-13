// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.perf

import java.util.concurrent.TimeUnit

import com.digitalasset.platform.PlatformApplications
import com.digitalasset.platform.sandbox.perf.TestHelper._
import org.openjdk.jmh.annotations._

import scala.concurrent.Await

abstract class CreatedStateBase extends PerfBenchState {
  override def config: PlatformApplications.Config =
    PlatformApplications.Config.default
      .withDarFile(darFile.toPath)

  @Param(Array("10", "100", "1000", "100000"))
  var n: Int = _

  var workflowId: String = null

  @Setup(Level.Invocation)
  def init(): Unit = {
    workflowId = uniqueId()
    sendCreates()

  }

  def sendCreates(): Unit
}

class RangeOfIntsCreatedState extends CreatedStateBase {
  override def sendCreates(): Unit =
    Await.result(rangeOfIntsCreateCommand(this, workflowId, n), setupTimeout)
}

class ListOfNIntsCreatedState extends CreatedStateBase {
  override def sendCreates(): Unit =
    Await.result(listUtilCreateCommand(this, workflowId), setupTimeout)
}

class LargeTransactionBench {

  @Benchmark
  def singleHugeContract(state: RangeOfIntsCreatedState): Unit =
    Await.result(
      rangeOfIntsExerciseCommand(state, state.workflowId, "ToListContainer", None),
      perfTestTimeout)

  //note that when running this with Postgres the bottleneck seems to originate from the fact the we traverse the huge
  //Transaction and execute SQL queries one after another. We could potentially partition the transaction so we can have batch queries instead.
  @Timeout(time = 20, timeUnit = TimeUnit.MINUTES) // we have a rare issue where this test runs extremely long with 100k contracts, making the test fail due to JMH timeout
  @Benchmark
  def manySmallContracts(state: RangeOfIntsCreatedState): Unit = {
    Await.result(
      rangeOfIntsExerciseCommand(state, state.workflowId, "ToListOfIntContainers", None),
      perfTestTimeout)
  }

  @Benchmark
  def listOfNInts(state: ListOfNIntsCreatedState): Unit =
    Await.result(
      listUtilExerciseSizeCommand(state, listUtilTemplateId, state.workflowId, state.n),
      perfTestTimeout)

}
