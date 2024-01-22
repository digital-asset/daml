// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.daml.ledger.api.v1.transaction.TreeEvent
import com.daml.ledger.api.v1.transaction.TreeEvent.Kind.{Created, Exercised}
import com.daml.ledger.api.v1.value.Value
import com.daml.ledger.api.v2.transaction.TransactionTree as TransactionTreeV2
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.console.{
  InstanceReferenceX,
  LocalParticipantReferenceCommon,
  LocalParticipantReferenceX,
}
import com.digitalasset.canton.participant.ParticipantNodeCommon
import com.digitalasset.canton.participant.admin.inspection.SyncStateInspection
import com.digitalasset.canton.participant.sync.{LedgerSyncEvent, TimestampedEvent}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{DomainAlias, LfTimestamp}
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
      domain: DomainAlias,
      pr: SyncStateInspection,
      limit: Int = 100,
  ): GrabbedCounts = {
    implicit val traceContext: TraceContext = TraceContext.empty
    val pcsCount = pr.findContracts(domain, None, None, None, limit = limit).length
    val acceptedTransactionCount = pr.findAcceptedTransactions(Some(domain)).length
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
      domainAlias: DomainAlias,
      participant: LocalParticipantReferenceX,
      limit: Int = 100,
  ): GrabbedCounts = {
    val pcsCount = participant.testing.pcs_search(domainAlias, limit = limit).length
    val acceptedTransactionCount =
      participant.testing.transaction_search(Some(domainAlias), limit = limit).length
    mkGrabCounts(pcsCount, acceptedTransactionCount, limit)
  }

  def expectedGrabbedCountsForBong(levels: Long, validators: Int = 0): GrabbedCounts = {
    // 2^(n+2) - 3 contracts plus input ping (last collapse changes to pong) plus PingProposals for validator
    val contracts = (math.pow(2, levels + 2d) - 3 + 1).toInt + validators
    // 2^(n+1) + 1 + validator events expected
    val events = (math.pow(2, levels + 1d) + 1).toInt + validators
    GrabbedCounts(contracts, events)
  }

  def assertIncreasingRecordTime[ParticipantNodeT <: ParticipantNodeCommon](
      domain: DomainAlias,
      pr: LocalParticipantReferenceCommon[ParticipantNodeT],
  ): Unit =
    assertIncreasingRecordTime(domain, alias => pr.testing.event_search(alias))

  def assertIncreasingRecordTime(
      domain: DomainAlias,
      events: Option[DomainAlias] => Seq[(String, TimestampedEvent)],
  ): Unit = {
    def assertIsSorted(s: Seq[(LfTimestamp, LedgerSyncEvent)]): Unit =
      s.sliding(2).collect { case Seq(x, y) =>
        assert(
          x._1 <= y._1,
          show"events ${x._2} and ${y._2} in event log for domain ${domain.unwrap.singleQuoted} not ordered by record time",
        )
      }

    val eventsWithRecordTime = events(Some(domain)).map { case (_offset, event) =>
      (event.timestamp.toLf, event.event)
    }
    assertIsSorted(eventsWithRecordTime)
  }

  def extractSubmissionResult(tree: TransactionTreeV2): Value.Sum = {
    require(
      tree.rootEventIds.size == 1,
      s"Received transaction with not exactly one root node: $tree",
    )
    tree.eventsById(tree.rootEventIds.head).kind match {
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

    @tailrec def pollTestCode(): T = {
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
    }
    pollTestCode()
  }

  def runOnAllInitializedDomainsForAllOwners(
      initializedDomains: Map[DomainAlias, InitializedDomain],
      run: (InstanceReferenceX, InitializedDomain) => Unit,
      topologyAwaitIdle: Boolean,
  ): Unit =
    initializedDomains.foreach { case (_, initializedDomain) =>
      if (topologyAwaitIdle) {
        initializedDomain.domainOwners.foreach(_.topology.synchronisation.await_idle())
      }
      initializedDomain.domainOwners.foreach(run(_, initializedDomain))
    }
}
