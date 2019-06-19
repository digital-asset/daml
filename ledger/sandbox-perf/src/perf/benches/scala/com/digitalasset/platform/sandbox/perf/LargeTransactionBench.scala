// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.perf

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
  @Benchmark
  def manySmallContracts(state: RangeOfIntsCreatedState): Unit = {
    try {
      Await.result(
        rangeOfIntsExerciseCommand(state, state.workflowId, "ToListOfIntContainers", None),
        perfTestTimeout)
    } catch {
      case t: Throwable => //TODO: this is just temporary to debug flakiness
        println(s"manySmallContracts bench blew up! ${t.getMessage} ")
        t.printStackTrace()
        throw t
    }
  }

  @Benchmark
  def listOfNInts(state: ListOfNIntsCreatedState): Unit =
    Await.result(
      listUtilExerciseSizeCommand(state, listUtilTemplateId, state.workflowId, state.n),
      perfTestTimeout)

}
