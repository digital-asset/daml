// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.error.MediatorError.MalformedMessage
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  InformeeMessage,
  RootHashMessage,
  SerializedRootHashMessagePayload,
  Verdict,
}
import com.digitalasset.canton.protocol.{ExampleTransactionFactory, RequestId, TestDomainParameters}
import com.digitalasset.canton.sequencing.client.{
  SendAsyncClientError,
  SendCallback,
  SendType,
  SequencerClientSend,
}
import com.digitalasset.canton.sequencing.protocol.{
  AggregationRule,
  Batch,
  MemberRecipient,
  MessageId,
  OpenEnvelope,
  Recipients,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{
  DomainId,
  MediatorGroup,
  MediatorId,
  MediatorRef,
  ParticipantId,
  TestingIdentityFactory,
  TestingIdentityFactoryX,
  TestingTopology,
  TestingTopologyX,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, ProtocolVersionChecksAsyncWordSpec}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class DefaultVerdictSenderTest
    extends AsyncWordSpec
    with ProtocolVersionChecksAsyncWordSpec
    with BaseTest {

  val activeMediator1 = MediatorId(UniqueIdentifier.tryCreate("mediator", "one"))
  val activeMediator2 = MediatorId(UniqueIdentifier.tryCreate("mediator", "two"))
  val passiveMediator3 = MediatorId(UniqueIdentifier.tryCreate("mediator", "three"))

  val mediatorGroup: MediatorGroup = MediatorGroup(
    index = NonNegativeInt.zero,
    active = Seq(
      activeMediator1,
      activeMediator2,
    ),
    passive = Seq(
      passiveMediator3
    ),
    threshold = PositiveInt.tryCreate(2),
  )
  val expectedMediatorGroupAggregationRule = Some(
    AggregationRule(
      NonEmpty.mk(Seq, mediatorGroup.active(0), mediatorGroup.active.tail *),
      PositiveInt.tryCreate(2),
      testedProtocolVersion,
    )
  )

  "DefaultVerdictSender" should {
    "work for pv <= dev" should {
      "send approvals" in {
        val tester = TestHelper(
          mediatorId = activeMediator1,
          transactionMediatorRef = MediatorRef(activeMediator1),
        )
        tester.sendApproval() map { _ =>
          tester.interceptedMessages should have size 1
        }
      }
      "send rejects" in {
        val tester = TestHelper(
          mediatorId = activeMediator1,
          transactionMediatorRef = MediatorRef(activeMediator1),
        )
        tester.sendReject() map { _ =>
          tester.interceptedMessages should have size 1
        }
      }
    }

    "for active mediators" should {
      "send approvals" onlyRunWithOrGreaterThan ProtocolVersion.CNTestNet in {
        val tester = TestHelper(
          mediatorId = activeMediator1,
          transactionMediatorRef = MediatorRef(mediatorGroup),
        )
        tester.sendApproval() map { _ =>
          tester.interceptedMessages should have size 1
        }
      }
      "send rejects" onlyRunWithOrGreaterThan ProtocolVersion.CNTestNet in {
        val tester = TestHelper(
          mediatorId = activeMediator1,
          transactionMediatorRef = MediatorRef(mediatorGroup),
        )
        tester.sendReject() map { _ =>
          tester.interceptedMessages should have size 1
        }
      }
    }

    "for passive mediators" should {
      "not send approvals" onlyRunWithOrGreaterThan ProtocolVersion.CNTestNet in {
        val tester = TestHelper(
          mediatorId = passiveMediator3,
          transactionMediatorRef = MediatorRef(mediatorGroup),
        )
        tester.sendApproval() map { _ =>
          tester.interceptedMessages should have size 0
        }
      }
      "not send rejects" onlyRunWithOrGreaterThan ProtocolVersion.CNTestNet in {
        val tester = TestHelper(
          mediatorId = passiveMediator3,
          transactionMediatorRef = MediatorRef(mediatorGroup),
        )
        tester.sendReject() map { _ =>
          tester.interceptedMessages should have size 0
        }
      }
    }

    "for requests to a singular mediator should set no aggregation rule" should {
      "for approvals" onlyRunWithOrGreaterThan ProtocolVersion.CNTestNet in {
        val tester = TestHelper(
          mediatorId = activeMediator1,
          transactionMediatorRef = MediatorRef(activeMediator1),
        )
        tester.sendApproval() map { _ =>
          tester.interceptedMessages should have size 1
          val (_, aggregationRule) = tester.interceptedMessages.loneElement
          aggregationRule shouldBe None
        }
      }
      "for rejects" onlyRunWithOrGreaterThan ProtocolVersion.CNTestNet in {
        val tester = TestHelper(
          mediatorId = activeMediator1,
          transactionMediatorRef = MediatorRef(activeMediator1),
        )
        tester.sendReject() map { _ =>
          tester.interceptedMessages should have size 1
          val (_, aggregationRule) = tester.interceptedMessages.loneElement
          aggregationRule shouldBe None
        }
      }
    }

    "for requests to a mediator group should set aggregation rule" should {
      "for approvals" onlyRunWithOrGreaterThan ProtocolVersion.CNTestNet in {
        val tester = TestHelper(
          mediatorId = activeMediator1,
          transactionMediatorRef = MediatorRef(mediatorGroup),
        )
        tester.sendApproval() map { _ =>
          tester.interceptedMessages should have size 1
          val (_, aggregationRule) = tester.interceptedMessages.loneElement
          aggregationRule shouldBe expectedMediatorGroupAggregationRule
        }
      }
      "for rejects" onlyRunWithOrGreaterThan ProtocolVersion.CNTestNet in {
        val tester = TestHelper(
          mediatorId = activeMediator1,
          transactionMediatorRef = MediatorRef(mediatorGroup),
        )
        tester.sendReject() map { _ =>
          tester.interceptedMessages should have size 1
          val (_, aggregationRule) = tester.interceptedMessages.loneElement
          aggregationRule shouldBe expectedMediatorGroupAggregationRule
        }
      }
    }
  }

  case class TestHelper(
      mediatorId: MediatorId,
      transactionMediatorRef: MediatorRef,
  ) {

    val domainId: DomainId = DomainId(
      UniqueIdentifier.tryFromProtoPrimitive("domain::test")
    )

    val factory =
      new ExampleTransactionFactory()(domainId = domainId, mediatorRef = transactionMediatorRef)
    val mediatorRef: MediatorRef = factory.mediatorRef
    val fullInformeeTree = factory.MultipleRootsAndViewNestings.fullInformeeTree
    val informeeMessage = InformeeMessage(fullInformeeTree)(testedProtocolVersion)
    val rootHashMessage = RootHashMessage(
      fullInformeeTree.transactionId.toRootHash,
      domainId,
      testedProtocolVersion,
      ViewType.TransactionViewType,
      SerializedRootHashMessagePayload.empty,
    )
    val participant: ParticipantId = ExampleTransactionFactory.submitterParticipant
    val rhmEnvelope = OpenEnvelope(
      rootHashMessage,
      Recipients.cc(transactionMediatorRef.toRecipient, MemberRecipient(participant)),
    )(testedProtocolVersion)

    val submitter = ExampleTransactionFactory.submitter
    val signatory = ExampleTransactionFactory.signatory
    val observer = ExampleTransactionFactory.observer

    val requestIdTs = CantonTimestamp.Epoch
    val requestId = RequestId(requestIdTs)
    val decisionTime = requestIdTs.plusSeconds(120)

    val initialDomainParameters = TestDomainParameters.defaultDynamic

    val domainSyncCryptoApi: DomainSyncCryptoClient =
      if (testedProtocolVersion >= ProtocolVersion.CNTestNet) {
        val topology = TestingTopologyX(
          Set(domainId),
          Map(
            submitter -> Map(participant -> ParticipantPermission.Confirmation),
            signatory ->
              Map(participant -> ParticipantPermission.Confirmation),
            observer ->
              Map(participant -> ParticipantPermission.Observation),
          ),
          Set(mediatorGroup),
        )

        val identityFactory = TestingIdentityFactoryX(
          topology,
          loggerFactory,
          dynamicDomainParameters = initialDomainParameters,
        )

        identityFactory.forOwnerAndDomain(mediatorId, domainId)
      } else {
        val topology = TestingTopology(
          Set(domainId),
          Map(
            submitter -> Map(participant -> ParticipantPermission.Confirmation),
            signatory ->
              Map(participant -> ParticipantPermission.Confirmation),
            observer ->
              Map(participant -> ParticipantPermission.Observation),
          ),
          Set(mediatorId),
        )

        val identityFactory = TestingIdentityFactory(
          topology,
          loggerFactory,
          dynamicDomainParameters = initialDomainParameters,
        )

        identityFactory.forOwnerAndDomain(mediatorId, domainId)
      }

    val interceptedMessages: java.util.concurrent.BlockingQueue[
      (Batch[DefaultOpenEnvelope], Option[AggregationRule])
    ] =
      new java.util.concurrent.LinkedBlockingQueue()

    val verdictSender = new DefaultVerdictSender(
      new SequencerClientSend {
        override def sendAsync(
            batch: Batch[DefaultOpenEnvelope],
            sendType: SendType,
            timestampOfSigningKey: Option[CantonTimestamp],
            maxSequencingTime: CantonTimestamp,
            messageId: MessageId,
            aggregationRule: Option[AggregationRule],
            callback: SendCallback,
        )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncClientError, Unit] = {
          interceptedMessages.add((batch, aggregationRule))
          EitherT.pure(())
        }

        override def generateMaxSequencingTime: CantonTimestamp = ???
      },
      domainSyncCryptoApi,
      mediatorId,
      testedProtocolVersion,
      loggerFactory,
    )

    def sendApproval(): Future[Unit] = {
      verdictSender.sendResult(
        requestId,
        informeeMessage,
        Verdict.Approve(testedProtocolVersion),
        decisionTime,
      )
    }

    def sendReject(): Future[Unit] = {
      verdictSender.sendReject(
        requestId,
        Some(informeeMessage),
        Seq(rhmEnvelope),
        MediatorVerdict
          .MediatorReject(MalformedMessage.Reject("Test failure"))
          .toVerdict(testedProtocolVersion),
        decisionTime,
      )
    }
  }

}
