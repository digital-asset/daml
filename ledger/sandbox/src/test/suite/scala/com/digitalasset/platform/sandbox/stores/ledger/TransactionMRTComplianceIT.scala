// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import java.time.Instant

import akka.stream.scaladsl.Sink
import com.daml.ledger.participant.state.v1.SubmissionResult
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.Transaction.{TContractId, NodeId, Value}
import com.digitalasset.daml.lf.transaction.{BlindingInfo, GenTransaction}
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  Resource,
  SuiteResourceManagementAroundEach
}
import com.digitalasset.ledger.backend.api.v1.{RejectionReason, TransactionSubmission}
import com.digitalasset.platform.sandbox.{LedgerResource, MetricsAround}
import com.digitalasset.platform.testing.MultiResourceBase
import org.scalatest.concurrent.{AsyncTimeLimitedTests, ScalaFutures}
import org.scalatest.time.Span
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.duration._
import scala.language.implicitConversions

sealed abstract class BackendType

object BackendType {

  case object InMemory extends BackendType

  case object Postgres extends BackendType

}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class TransactionMRTComplianceIT
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with MultiResourceBase[BackendType, Ledger]
    with SuiteResourceManagementAroundEach
    with AsyncTimeLimitedTests
    with ScalaFutures
    with Matchers
    with MetricsAround {

  override def timeLimit: Span = 60.seconds

  val ledgerId = Ref.LedgerIdString.assertFromString("ledgerId")
  val timeProvider = TimeProvider.Constant(Instant.EPOCH.plusSeconds(10))

  /** Overriding this provides an easy way to narrow down testing to a single implementation. */
  override protected def fixtureIdsEnabled: Set[BackendType] =
    Set(BackendType.InMemory, BackendType.Postgres)

  override protected def constructResource(index: Int, fixtureId: BackendType): Resource[Ledger] =
    fixtureId match {
      case BackendType.InMemory =>
        LedgerResource.inMemory(ledgerId, timeProvider)
      case BackendType.Postgres =>
        LedgerResource.postgres(ledgerId, timeProvider)
    }

  val LET = Instant.EPOCH.plusSeconds(2)
  val MRT = Instant.EPOCH.plusSeconds(5)

  "A Ledger" should {
    "reject transactions with a record time after the MRT" in allFixtures { ledger =>
      val emptyBlinding = BlindingInfo(Map.empty, Map.empty, Map.empty)
      val dummyTransaction =
        GenTransaction[NodeId, TContractId, Value[TContractId]](
          Map.empty,
          ImmArray.empty,
          Set.empty)
      val submission = TransactionSubmission(
        "cmdId",
        Some("wfid"),
        "submitter",
        LET,
        MRT,
        "appId",
        emptyBlinding,
        dummyTransaction)

      ledger.publishTransaction(submission).map(_ shouldBe SubmissionResult.Acknowledged)
      ledger
        .ledgerEntries(None)
        .runWith(Sink.head)
        .map(_._2)
        .map {
          _ should matchPattern {
            case LedgerEntry.Rejection(
                recordTime,
                "cmdId",
                "appId",
                "submitter",
                RejectionReason.TimedOut(_)) =>
          }
        }
    }
  }

  private implicit def toParty(s: String): Ref.Party = Ref.Party.assertFromString(s)

  private implicit def toLedgerString(s: String): Ref.LedgerString =
    Ref.LedgerString.assertFromString(s)

}
