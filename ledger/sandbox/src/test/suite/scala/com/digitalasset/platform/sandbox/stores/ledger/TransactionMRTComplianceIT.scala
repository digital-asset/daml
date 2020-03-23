// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import java.time.Instant

import akka.stream.scaladsl.Sink
import com.daml.ledger.participant.state.v1.{
  ParticipantId,
  SubmissionResult,
  SubmitterInfo,
  TransactionMeta
}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.transaction.{GenTransaction, Transaction}
import com.digitalasset.ledger.api.domain.{LedgerId, RejectionReason}
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  MultiResourceBase,
  Resource,
  SuiteResourceManagementAroundEach
}
import com.digitalasset.logging.LoggingContext.newLoggingContext
import com.digitalasset.platform.sandbox.stores.ledger.TransactionMRTComplianceIT._
import com.digitalasset.platform.sandbox.{LedgerResource, MetricsAround}
import com.digitalasset.platform.store.entries.LedgerEntry
import org.scalatest.concurrent.{AsyncTimeLimitedTests, ScalaFutures}
import org.scalatest.time.Span
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.collection.immutable.HashMap
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.implicitConversions

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

  override def timeLimit: Span = scaled(60.seconds)

  /** Overriding this provides an easy way to narrow down testing to a single implementation. */
  override protected def fixtureIdsEnabled: Set[BackendType] =
    Set(BackendType.InMemory, BackendType.Postgres)

  override protected def constructResource(index: Int, fixtureId: BackendType): Resource[Ledger] = {
    implicit val executionContext: ExecutionContext = system.dispatcher
    fixtureId match {
      case BackendType.InMemory =>
        LedgerResource.inMemory(ledgerId, participantId, timeProvider)
      case BackendType.Postgres =>
        newLoggingContext { implicit logCtx =>
          LedgerResource.postgres(getClass, ledgerId, participantId, timeProvider, metrics)
        }
    }
  }

  "A Ledger" should {
    "reject transactions with a record time after the MRT" in allFixtures { ledger =>
      val seed = Some(crypto.Hash.hashPrivateKey(this.getClass.getName))

      val dummyTransaction: Transaction.AbsTransaction =
        GenTransaction(HashMap.empty, ImmArray.empty)

      val submitterInfo = SubmitterInfo(
        submitter = Ref.Party.assertFromString("submitter"),
        applicationId = Ref.LedgerString.assertFromString("appId"),
        commandId = Ref.LedgerString.assertFromString("cmdId"),
        maxRecordTime = Time.Timestamp.assertFromInstant(MRT),
        deduplicateUntil = Instant.EPOCH
      )
      val transactionMeta = TransactionMeta(
        ledgerEffectiveTime = Time.Timestamp.assertFromInstant(LET),
        workflowId = Some(Ref.LedgerString.assertFromString("wfid")),
        submissionTime = Time.Timestamp.assertFromInstant(ST),
        submissionSeed = seed,
        optUsedPackages = None,
      )

      ledger
        .publishTransaction(submitterInfo, transactionMeta, dummyTransaction)
        .map(_ shouldBe SubmissionResult.Acknowledged)
      ledger
        .ledgerEntries(None, None)
        .runWith(Sink.head)
        .map(_._2)
        .map {
          _ should matchPattern {
            case LedgerEntry.Rejection(
                _,
                "cmdId",
                "appId",
                "submitter",
                RejectionReason.TimedOut(_)) =>
          }
        }
    }
  }

}

object TransactionMRTComplianceIT {

  private val ledgerId: LedgerId = LedgerId(Ref.LedgerString.assertFromString("ledgerId"))
  private val participantId: ParticipantId = Ref.ParticipantId.assertFromString("participantId")
  private val timeProvider = TimeProvider.Constant(Instant.EPOCH.plusSeconds(10))

  private val LET = Instant.EPOCH.plusSeconds(2)
  private val ST = LET.plusNanos(3)
  private val MRT = Instant.EPOCH.plusSeconds(5)

  private implicit def toParty(s: String): Ref.Party = Ref.Party.assertFromString(s)

  private implicit def toLedgerString(s: String): Ref.LedgerString =
    Ref.LedgerString.assertFromString(s)

  sealed abstract class BackendType

  object BackendType {

    case object InMemory extends BackendType

    case object Postgres extends BackendType

  }

}
