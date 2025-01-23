// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.daml.ledger.api.v2.transaction.TreeEvent.Kind.{Created, Exercised}
import com.daml.ledger.api.v2.transaction.{TransactionTree as TransactionTreeV2, TreeEvent}
import com.daml.ledger.api.v2.value.Value
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.console.{InstanceReference, LocalParticipantReference}
import com.digitalasset.canton.ledger.api.util.TransactionTreeOps.*
import com.digitalasset.canton.participant.admin.inspection.SyncStateInspection
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.exceptions.TestFailedException

import scala.annotation.tailrec
import scala.concurrent.duration.{Duration, FiniteDuration}

object IntegrationTestUtilities {
  import com.digitalasset.canton.console.ConsoleEnvironment.Implicits.*

  final case class GrabbedCounts(pcsCount: Int, acceptedTransactionCount: Int) {
    def plus(other: GrabbedCounts): GrabbedCounts =
      GrabbedCounts(
        this.pcsCount + other.pcsCount,
        this.acceptedTransactionCount + other.acceptedTransactionCount,
      )

    def minus(other: GrabbedCounts): GrabbedCounts =
      GrabbedCounts(
        this.pcsCount - other.pcsCount,
        this.acceptedTransactionCount - other.acceptedTransactionCount,
      )

    def maxCount: Int = pcsCount max acceptedTransactionCount
  }

  def grabCountsRemote(
      synchronizerAlias: SynchronizerAlias,
      pr: SyncStateInspection,
      limit: Int = 100,
  ): GrabbedCounts = {
    implicit val traceContext: TraceContext = TraceContext.empty
    val pcsCount = pr.findContracts(synchronizerAlias, None, None, None, limit = limit).length
    val acceptedTransactionCount = pr.acceptedTransactionCount(synchronizerAlias)
    mkGrabCounts(pcsCount, acceptedTransactionCount, limit)
  }

  private def mkGrabCounts(
      pcsCount: Int,
      acceptedTransactionCount: Int,
      limit: Int,
  ): GrabbedCounts = {
    // help others to not run into the same issue ...
    require(
      acceptedTransactionCount < limit,
      "transaction count is the same number as the current limit. i am sure you want to increase the limit here",
    )
    require(
      pcsCount < limit,
      "transaction count is the same number as the current limit. i am sure you want to increase the limit here",
    )
    GrabbedCounts(pcsCount, acceptedTransactionCount)
  }

  def grabCounts(
      synchronizerAlias: SynchronizerAlias,
      participant: LocalParticipantReference,
      limit: Int = 100,
  ): GrabbedCounts = {
    val pcsCount = participant.testing.pcs_search(synchronizerAlias, limit = limit).length
    val acceptedTransactionCount =
      Integer.min(
        participant.testing.state_inspection
          .acceptedTransactionCount(synchronizerAlias)(TraceContext.empty),
        limit,
      )
    mkGrabCounts(pcsCount, acceptedTransactionCount, limit)
  }

  def expectedGrabbedCountsForBong(levels: Int, validators: Int = 0): GrabbedCounts = {
    // 2^(n+2) - 3 contracts plus input BongProposal (last collapse changes to bong) for validator
    val contracts = (math.pow(2, levels + 2d) - 3).toInt + Math.max(1, validators)
    // 2^(n+1) + validator events expected
    val events = (math.pow(2, levels + 1d)).toInt + Math.max(1, validators)
    GrabbedCounts(contracts, events)
  }

  def assertIncreasingRecordTime(pr: LocalParticipantReference): Unit =
    pr.testing.state_inspection.verifyLapiStoreIntegrity()(TraceContext.empty)

  def extractSubmissionResult(tree: TransactionTreeV2): Value.Sum = {
    require(
      tree.rootNodeIds().sizeIs == 1,
      s"Received transaction with not exactly one root node: $tree",
    )
    tree.eventsById(tree.rootNodeIds().head).kind match {
      case Created(created) => Value.Sum.ContractId(created.contractId)
      case Exercised(exercised) =>
        val Value(result) = exercised.exerciseResult.getOrElse(
          throw new RuntimeException("Unable to exercise choice.")
        )
        result
      case TreeEvent.Kind.Empty =>
        throw new IllegalArgumentException(s"Received transaction with empty event kind: $tree")
    }
  }

  def poll[T](timeout: FiniteDuration, interval: FiniteDuration)(testCode: => T): T = {
    require(timeout >= Duration.Zero)
    require(interval >= Duration.Zero)
    val deadline = timeout.fromNow

    @tailrec def pollTestCode(): T =
      if (deadline.hasTimeLeft()) {
        try {
          testCode
        } catch {
          case _: TestFailedException =>
            Threading.sleep(interval.toMillis)
            pollTestCode()
        }
      } else {
        testCode
      }
    pollTestCode()
  }

  def runOnAllInitializedSynchronizersForAllOwners(
      initializedSynchronizers: Map[SynchronizerAlias, InitializedSynchronizer],
      run: (InstanceReference, InitializedSynchronizer) => Unit,
      topologyAwaitIdle: Boolean,
  ): Unit =
    initializedSynchronizers.foreach { case (_, initializedSynchronizer) =>
      if (topologyAwaitIdle) {
        initializedSynchronizer.synchronizerOwners.foreach(_.topology.synchronisation.await_idle())
      }
      initializedSynchronizer.synchronizerOwners.foreach(run(_, initializedSynchronizer))
    }
}
