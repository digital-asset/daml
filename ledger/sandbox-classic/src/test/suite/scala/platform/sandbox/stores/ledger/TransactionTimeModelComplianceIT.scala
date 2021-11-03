// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger

import java.time.{Instant, Duration => JDuration}
import java.util.UUID

import akka.stream.scaladsl.Sink
import com.daml.api.util.TimeProvider
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  MultiResourceBase,
  Resource,
  SuiteResourceManagementAroundEach,
}
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.ResourceContext
import com.daml.lf.crypto
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.platform.sandbox.stores.ledger.TransactionTimeModelComplianceIT._
import com.daml.platform.sandbox.{LedgerResource, MetricsAround}
import com.daml.platform.server.api.validation.ErrorFactories
import org.mockito.MockitoSugar
import org.scalatest.concurrent.{AsyncTimeLimitedTests, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.Span
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{Assertion, OptionValues}

import scala.concurrent.Future
import scala.concurrent.duration._

class TransactionTimeModelComplianceIT
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with MultiResourceBase[BackendType, Ledger]
    with SuiteResourceManagementAroundEach
    with AsyncTimeLimitedTests
    with ScalaFutures
    with Matchers
    with OptionValues
    with MetricsAround
    with MockitoSugar {

  override def timeLimit: Span = scaled(60.seconds)

  /** Overriding this provides an easy way to narrow down testing to a single implementation. */
  override protected def fixtureIdsEnabled: Set[BackendType] =
    Set(BackendType.InMemory, BackendType.Postgres)

  override protected def constructResource(index: Int, fixtureId: BackendType): Resource[Ledger] = {
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    fixtureId match {
      case BackendType.InMemory =>
        LedgerResource.inMemory(ledgerId, timeProvider, mock[ErrorFactories])
      case BackendType.Postgres =>
        LedgerResource.postgres(getClass, ledgerId, timeProvider, metrics, mock[ErrorFactories])
    }
  }

  private[this] val submissionSeed = crypto.Hash.hashPrivateKey(this.getClass.getName)

  private[this] def publishConfig(
      ledger: Ledger,
      recordTime: Instant,
      generation: Long,
      minSkew: JDuration,
      maxSkew: JDuration,
  ): Future[Configuration] = {
    val config = Configuration(
      generation = generation,
      timeModel = LedgerTimeModel(JDuration.ZERO, minSkew, maxSkew).get,
      maxDeduplicationTime = JDuration.ofSeconds(10),
    )
    ledger
      .publishConfiguration(
        Time.Timestamp.assertFromInstant(recordTime.plusSeconds(3600)),
        UUID.randomUUID().toString,
        config,
      )
      .map(_ => config)
  }

  private[this] def publishTxAt(
      ledger: Ledger,
      ledgerTime: Instant,
      commandId: String,
      configuration: Configuration,
  ) = {
    val dummyTransaction = TransactionBuilder.EmptySubmitted

    val submitterInfo = state.SubmitterInfo(
      actAs = List(Ref.Party.assertFromString("submitter")),
      applicationId = Ref.ApplicationId.assertFromString("appId"),
      commandId = Ref.CommandId.assertFromString(commandId + UUID.randomUUID().toString),
      deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(JDuration.ZERO),
      submissionId = None,
      ledgerConfiguration = configuration,
    )
    val transactionMeta = state.TransactionMeta(
      ledgerEffectiveTime = Time.Timestamp.assertFromInstant(ledgerTime),
      workflowId = Some(Ref.WorkflowId.assertFromString("wfid")),
      submissionTime = Time.Timestamp.assertFromInstant(ledgerTime.plusNanos(3)),
      submissionSeed = submissionSeed,
      optUsedPackages = None,
      optNodeSeeds = None,
      optByKeyNodes = None,
    )

    val offset = ledger.ledgerEnd()

    for {
      submissionResult <- ledger.publishTransaction(
        submitterInfo,
        transactionMeta,
        dummyTransaction,
      )
      completion <- ledger
        .completions(
          Some(offset),
          None,
          com.daml.ledger.api.domain.ApplicationId(submitterInfo.applicationId),
          submitterInfo.actAs.toSet,
        )
        .filter(_._2.completions.head.commandId == submitterInfo.commandId)
        .runWith(Sink.head)
    } yield {
      submissionResult shouldBe state.SubmissionResult.Acknowledged
      completion._2.completions.head
    }
  }

  private[this] def expectInvalidLedgerTime(completion: Completion): Assertion = {
    completion.status.value.code shouldBe aborted
  }

  private[this] def expectValidTx(completion: Completion): Assertion =
    completion.status.value.code shouldBe ok

  "A Ledger" should {
    "reject transactions if there is no ledger config" in allFixtures { ledger =>
      val ledgerTime = recordTime

      for {
        r1 <- publishTxAt(
          ledger = ledger,
          ledgerTime = ledgerTime,
          commandId = "lt-valid",
          configuration = null,
        )
      } yield {
        expectInvalidLedgerTime(r1)
      }
    }
    "accept transactions with ledger time that is right" in allFixtures { ledger =>
      val ledgerTime = recordTime

      for {
        config <- publishConfig(
          ledger,
          recordTime,
          1,
          JDuration.ofSeconds(1),
          JDuration.ofSeconds(1),
        )
        r1 <- publishTxAt(
          ledger = ledger,
          ledgerTime = ledgerTime,
          commandId = "lt-valid",
          configuration = config,
        )
      } yield {
        expectValidTx(r1)
      }
    }
    "reject transactions with ledger time that is too low" in allFixtures { ledger =>
      val minSkew = JDuration.ofSeconds(1)
      val maxSkew = JDuration.ofDays(1)
      val ledgerTime = recordTime.minus(minSkew).minusSeconds(1)

      for {
        config <- publishConfig(ledger, recordTime, 1, minSkew, maxSkew)
        r1 <- publishTxAt(
          ledger = ledger,
          ledgerTime = ledgerTime,
          commandId = "lt-low",
          configuration = config,
        )
      } yield {
        expectInvalidLedgerTime(r1)
      }
    }
    "reject transactions with ledger time that is too high" in allFixtures { ledger =>
      val minSkew = JDuration.ofDays(1)
      val maxSkew = JDuration.ofSeconds(1)
      val ledgerTime = recordTime.plus(maxSkew).plusSeconds(1)

      for {
        config <- publishConfig(ledger, recordTime, 1, minSkew, maxSkew)
        r1 <- publishTxAt(
          ledger = ledger,
          ledgerTime = ledgerTime,
          commandId = "lt-high",
          configuration = config,
        )
      } yield {
        expectInvalidLedgerTime(r1)
      }
    }
    "reject transactions after ledger config changes" in allFixtures { ledger =>
      val largeSkew = JDuration.ofDays(1)
      val smallSkew = JDuration.ofSeconds(1)
      val ledgerTime = recordTime.plus(largeSkew)

      for {
        config1 <- publishConfig(ledger, recordTime, 1, largeSkew, largeSkew)
        r1 <- publishTxAt(
          ledger = ledger,
          ledgerTime = ledgerTime,
          commandId = "lt-before",
          configuration = config1,
        )
        config2 <- publishConfig(ledger, recordTime, 2, smallSkew, smallSkew)
        r2 <- publishTxAt(
          ledger = ledger,
          ledgerTime = ledgerTime,
          commandId = "lt-after",
          configuration = config2,
        )
      } yield {
        expectValidTx(r1)
        expectInvalidLedgerTime(r2)
      }
    }
  }

}

object TransactionTimeModelComplianceIT {

  private val recordTime = Instant.now

  private val ledgerId = LedgerId("ledgerId")
  private val timeProvider = TimeProvider.Constant(recordTime)

  sealed abstract class BackendType

  object BackendType {

    case object InMemory extends BackendType

    case object Postgres extends BackendType

  }

  private val ok = io.grpc.Status.Code.OK.value()
  private val aborted = io.grpc.Status.Code.ABORTED.value()

}
