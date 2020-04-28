// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger

import java.time.Instant

import akka.stream.scaladsl.Sink
import com.daml.ledger.participant.state.v1.{
  ParticipantId,
  SubmissionResult,
  SubmitterInfo,
  TimeModel,
  TransactionMeta
}
import com.daml.api.util.TimeProvider
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  MultiResourceBase,
  Resource,
  SuiteResourceManagementAroundEach
}
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.participant.state.v1._
import com.daml.lf.crypto
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.transaction.{GenTransaction, Transaction}
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.platform.sandbox.stores.ledger.TransactionTimeModelComplianceIT._
import com.daml.platform.sandbox.{LedgerResource, MetricsAround}
import org.scalatest.concurrent.{AsyncTimeLimitedTests, ScalaFutures}
import org.scalatest.time.Span
import org.scalatest.{Assertion, AsyncWordSpec, Matchers, OptionValues}

import scala.collection.immutable.HashMap
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.implicitConversions

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class TransactionTimeModelComplianceIT
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with MultiResourceBase[BackendType, Ledger]
    with SuiteResourceManagementAroundEach
    with AsyncTimeLimitedTests
    with ScalaFutures
    with Matchers
    with OptionValues
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
          LedgerResource.postgres(
            getClass,
            ledgerId,
            participantId,
            timeProvider,
            metrics,
          )
        }
    }
  }

  private[this] val submissionSeed = Some(crypto.Hash.hashPrivateKey(this.getClass.getName))

  private[this] def publishTxAt(ledger: Ledger, ledgerTime: Instant, commandId: String) = {
    val dummyTransaction: Transaction.AbsTransaction =
      GenTransaction(HashMap.empty, ImmArray.empty)

    val submitterInfo = SubmitterInfo(
      submitter = Ref.Party.assertFromString("submitter"),
      applicationId = Ref.LedgerString.assertFromString("appId"),
      commandId = Ref.LedgerString.assertFromString(commandId),
      deduplicateUntil = Instant.EPOCH
    )
    val transactionMeta = TransactionMeta(
      ledgerEffectiveTime = Time.Timestamp.assertFromInstant(ledgerTime),
      workflowId = Some(Ref.LedgerString.assertFromString("wfid")),
      submissionTime = Time.Timestamp.assertFromInstant(ledgerTime.plusNanos(3)),
      submissionSeed = submissionSeed,
      optUsedPackages = None,
      optNodeSeeds = None,
      optByKeyNodes = None
    )

    val offset = ledger.ledgerEnd

    for {
      submissionResult <- ledger.publishTransaction(
        submitterInfo,
        transactionMeta,
        dummyTransaction)
      completion <- ledger
        .completions(
          Some(offset),
          None,
          com.daml.ledger.api.domain.ApplicationId(submitterInfo.applicationId),
          Set(submitterInfo.submitter)
        )
        .filter(_._2.completions.head.commandId == submitterInfo.commandId)
        .runWith(Sink.head)
    } yield {
      submissionResult shouldBe SubmissionResult.Acknowledged
      completion._2.completions.head
    }
  }

  private[this] def expectInvalidLedgerTime(completion: Completion): Assertion = {
    completion.status.value.code shouldBe aborted
  }

  private[this] def expectValidTx(completion: Completion): Assertion =
    completion.status.value.code shouldBe ok

  "A Ledger" should {
    "accept transactions with ledger time that is right" in allFixtures { ledger =>
      val ledgerTime = recordTime
      publishTxAt(ledger, ledgerTime, "lt-valid").flatMap(expectValidTx)
    }
    "reject transactions with ledger time that is too low" in allFixtures { ledger =>
      val ledgerTime = recordTime.minus(TimeModel.reasonableDefault.minSkew).minusSeconds(1)
      publishTxAt(ledger, ledgerTime, "lt-low").flatMap(expectInvalidLedgerTime)
    }
    "reject transactions with ledger time that is too high" in allFixtures { ledger =>
      val ledgerTime = recordTime.plus(TimeModel.reasonableDefault.maxSkew).plusSeconds(1)
      publishTxAt(ledger, ledgerTime, "lt-high").flatMap(expectInvalidLedgerTime)
    }
  }

}

object TransactionTimeModelComplianceIT {

  private val recordTime = Instant.now

  private val ledgerId: LedgerId = LedgerId(Ref.LedgerString.assertFromString("ledgerId"))
  private val participantId: ParticipantId = Ref.ParticipantId.assertFromString("participantId")
  private val timeProvider = TimeProvider.Constant(recordTime)

  private implicit def toParty(s: String): Ref.Party = Ref.Party.assertFromString(s)

  private implicit def toLedgerString(s: String): Ref.LedgerString =
    Ref.LedgerString.assertFromString(s)

  sealed abstract class BackendType

  object BackendType {

    case object InMemory extends BackendType

    case object Postgres extends BackendType

  }

  private val ok = io.grpc.Status.Code.OK.value()
  private val aborted = io.grpc.Status.Code.ABORTED.value()

}
