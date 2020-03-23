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
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.transaction.Node._
import com.digitalasset.daml.lf.transaction.{GenTransaction, Transaction}
import com.digitalasset.daml.lf.value.Value.{
  AbsoluteContractId,
  ContractInst,
  ValueText,
  VersionedValue
}
import com.digitalasset.daml.lf.value.ValueVersions
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  MultiResourceBase,
  Resource,
  SuiteResourceManagementAroundEach
}
import com.digitalasset.logging.LoggingContext.newLoggingContext
import com.digitalasset.platform.sandbox.stores.ledger.ImplicitPartyAdditionIT._
import com.digitalasset.platform.sandbox.{LedgerResource, MetricsAround}
import org.scalatest.concurrent.{AsyncTimeLimitedTests, ScalaFutures}
import org.scalatest.time.Span
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.collection.immutable.HashMap
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

class ImplicitPartyAdditionIT
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
    "implicitly add parties mentioned in a transaction" in allFixtures { ledger =>
      for {
        createResult <- publishSingleNodeTx(
          ledger,
          "create-signatory",
          "CmdId1",
          NodeCreate(
            nodeSeed = None,
            coid = AbsoluteContractId("cId1"),
            coinst = ContractInst(
              templateId1,
              textValue("some text"),
              "agreement"
            ),
            optLocation = None,
            signatories = Set("create-signatory"),
            stakeholders = Set("create-stakeholder"),
            key = Some(KeyWithMaintainers(textValue("some text"), Set("create-signatory")))
          )
        )
        exerciseResult <- publishSingleNodeTx(
          ledger,
          "exercise-signatory",
          "CmdId2",
          NodeExercises(
            nodeSeed = None,
            targetCoid = AbsoluteContractId("cId1"),
            templateId = templateId1,
            choiceId = Ref.ChoiceName.assertFromString("choice"),
            optLocation = None,
            consuming = false,
            actingParties = Set("exercise-signatory"),
            chosenValue = textValue("choice value"),
            stakeholders = Set("exercise-stakeholder"),
            signatories = Set("exercise-signatory"),
            controllers = Set("exercise-signatory"),
            children = ImmArray.empty,
            exerciseResult = None,
            key = None
          )
        )
        fetchResult <- publishSingleNodeTx(
          ledger,
          "fetch-signatory",
          "CmdId3",
          NodeFetch(
            AbsoluteContractId("cId1"),
            templateId1,
            None,
            Some(Set("fetch-acting-party")),
            Set("fetch-signatory"),
            Set("fetch-signatory")
          )
        )
        // Wait until both transactions have been processed
        _ <- ledger
          .ledgerEntries(None, None)
          .take(2)
          .runWith(Sink.seq)
        parties <- ledger.listKnownParties()
      } yield {
        createResult shouldBe SubmissionResult.Acknowledged
        exerciseResult shouldBe SubmissionResult.Acknowledged
        fetchResult shouldBe SubmissionResult.Acknowledged

        parties.exists(d => d.party == "create-signatory") shouldBe true
        parties.exists(d => d.party == "create-stakeholder") shouldBe true

        parties.exists(d => d.party == "exercise-signatory") shouldBe true
        parties.exists(d => d.party == "exercise-stakeholder") shouldBe true

        parties.exists(d => d.party == "fetch-acting-party") shouldBe true
        parties.exists(d => d.party == "fetch-signatory") shouldBe true
      }
    }
  }

  private implicit def toParty(s: String): Ref.Party = Ref.Party.assertFromString(s)

  private implicit def toLedgerString(s: String): Ref.LedgerString =
    Ref.LedgerString.assertFromString(s)

}

object ImplicitPartyAdditionIT {

  private val ledgerId: LedgerId = LedgerId("ledgerId")
  private val participantId: ParticipantId = Ref.ParticipantId.assertFromString("participantId")
  private val timeProvider = TimeProvider.Constant(Instant.EPOCH.plusSeconds(10))

  private val LET = Instant.EPOCH.plusSeconds(10)
  private val MRT = Instant.EPOCH.plusSeconds(10)
  private val DeduplicateUntil = Instant.now.plusSeconds(3600)

  private val templateId1: Ref.Identifier = Ref.Identifier(
    Ref.PackageId.assertFromString("packageId"),
    Ref.QualifiedName(
      Ref.ModuleName.assertFromString("moduleName"),
      Ref.DottedName.assertFromString("name")
    )
  )

  private def textValue(t: String) =
    VersionedValue(ValueVersions.acceptedVersions.head, ValueText(t))

  private def publishSingleNodeTx(
      ledger: Ledger,
      submitter: String,
      commandId: String,
      node: Transaction.AbsNode,
  ): Future[SubmissionResult] = {
    val event1: Transaction.NodeId = Transaction.NodeId(0)

    val let = Time.Timestamp.assertFromInstant(LET)

    val transaction: Transaction.AbsTransaction = GenTransaction(
      HashMap(event1 -> node),
      ImmArray(event1),
    )

    val submitterInfo = SubmitterInfo(
      Ref.Party.assertFromString(submitter),
      Ref.LedgerString.assertFromString("appId"),
      Ref.LedgerString.assertFromString(commandId),
      Time.Timestamp.assertFromInstant(MRT),
      DeduplicateUntil,
    )

    val transactionMeta = TransactionMeta(
      ledgerEffectiveTime = let,
      workflowId = Some(Ref.LedgerString.assertFromString("wfid")),
      submissionTime = let.addMicros(1000),
      submissionSeed = None,
      optUsedPackages = None,
    )

    ledger.publishTransaction(submitterInfo, transactionMeta, transaction)
  }

  sealed abstract class BackendType

  object BackendType {

    case object InMemory extends BackendType

    case object Postgres extends BackendType

  }

}
