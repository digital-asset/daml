// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, Signature}
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
import com.digitalasset.canton.sequencing.client.TestSequencerClientSend
import com.digitalasset.canton.sequencing.protocol.{
  AggregationRule,
  Batch,
  MediatorGroupRecipient,
  MemberRecipient,
  OpenEnvelope,
  Recipients,
}
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{
  DomainId,
  MediatorGroup,
  MediatorId,
  ParticipantId,
  TestingIdentityFactory,
  TestingTopology,
  UniqueIdentifier,
}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, ProtocolVersionChecksAsyncWordSpec}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

class DefaultVerdictSenderTest
    extends AsyncWordSpec
    with ProtocolVersionChecksAsyncWordSpec
    with BaseTest {

  private val activeMediator1 = MediatorId(UniqueIdentifier.tryCreate("mediator", "one"))
  private val activeMediator2 = MediatorId(UniqueIdentifier.tryCreate("mediator", "two"))
  private val passiveMediator3 = MediatorId(UniqueIdentifier.tryCreate("mediator", "three"))

  private val mediatorGroupRecipient = MediatorGroupRecipient(MediatorGroupIndex.zero)
  private val mediatorGroup: MediatorGroup = MediatorGroup(
    index = mediatorGroupRecipient.group,
    active = NonEmpty.mk(Seq, activeMediator1, activeMediator2),
    passive = Seq(
      passiveMediator3
    ),
    threshold = PositiveInt.tryCreate(2),
  )
  private val expectedMediatorGroupAggregationRule = Some(
    AggregationRule(
      NonEmpty.mk(Seq, mediatorGroup.active(0), mediatorGroup.active.tail*),
      PositiveInt.tryCreate(2),
      testedProtocolVersion,
    )
  )

  "DefaultVerdictSender" should {
    "work for pv <= dev" should {
      "send approvals" in {
        val tester = TestHelper(
          mediatorId = activeMediator1,
          transactionMediatorGroup = mediatorGroupRecipient,
        )
        tester.sendApproval() map { _ =>
          tester.interceptedMessages should have size 1
        }
      }
      "send rejects" in {
        val tester = TestHelper(
          mediatorId = activeMediator1,
          transactionMediatorGroup = mediatorGroupRecipient,
        )
        tester.sendReject() map { _ =>
          tester.interceptedMessages should have size 1
        }
      }
    }

    "for active mediators" should {
      "send approvals" in {
        val tester = TestHelper(
          mediatorId = activeMediator1,
          transactionMediatorGroup = mediatorGroupRecipient,
        )
        tester.sendApproval() map { _ =>
          tester.interceptedMessages should have size 1
        }
      }
      "send rejects" in {
        val tester = TestHelper(
          mediatorId = activeMediator1,
          transactionMediatorGroup = mediatorGroupRecipient,
        )
        tester.sendReject() map { _ =>
          tester.interceptedMessages should have size 1
        }
      }
    }

    "for passive mediators" should {
      "not send approvals" in {
        val tester = TestHelper(
          mediatorId = passiveMediator3,
          transactionMediatorGroup = mediatorGroupRecipient,
        )
        tester.sendApproval() map { _ =>
          tester.interceptedMessages should have size 0
        }
      }
      "not send rejects" in {
        val tester = TestHelper(
          mediatorId = passiveMediator3,
          transactionMediatorGroup = mediatorGroupRecipient,
        )
        tester.sendReject() map { _ =>
          tester.interceptedMessages should have size 0
        }
      }
    }

    "for requests to a mediator group should set aggregation rule" should {
      "for approvals" in {
        val tester = TestHelper(
          mediatorId = activeMediator1,
          transactionMediatorGroup = mediatorGroupRecipient,
        )
        tester.sendApproval() map { _ =>
          tester.interceptedMessages should have size 1
          val (_, aggregationRule) = tester.interceptedMessages.loneElement
          aggregationRule shouldBe expectedMediatorGroupAggregationRule
        }
      }
      "for rejects" in {
        val tester = TestHelper(
          mediatorId = activeMediator1,
          transactionMediatorGroup = mediatorGroupRecipient,
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
      transactionMediatorGroup: MediatorGroupRecipient,
  ) {

    val domainId: DomainId = DomainId(
      UniqueIdentifier.tryFromProtoPrimitive("domain::test")
    )
    val testTopologyTimestamp = CantonTimestamp.Epoch

    val factory =
      new ExampleTransactionFactory()(domainId = domainId, mediatorGroup = transactionMediatorGroup)
    val mediatorRecipient: MediatorGroupRecipient = factory.mediatorGroup
    val fullInformeeTree = factory.MultipleRootsAndViewNestings.fullInformeeTree
    val informeeMessage =
      InformeeMessage(fullInformeeTree, Signature.noSignature)(testedProtocolVersion)
    val rootHashMessage = RootHashMessage(
      fullInformeeTree.transactionId.toRootHash,
      domainId,
      testedProtocolVersion,
      ViewType.TransactionViewType,
      testTopologyTimestamp,
      SerializedRootHashMessagePayload.empty,
    )
    val participant: ParticipantId = ExampleTransactionFactory.submittingParticipant
    val rhmEnvelope = OpenEnvelope(
      rootHashMessage,
      Recipients.cc(transactionMediatorGroup, MemberRecipient(participant)),
    )(testedProtocolVersion)

    val submitter = ExampleTransactionFactory.submitter
    val signatory = ExampleTransactionFactory.signatory
    val observer = ExampleTransactionFactory.observer

    val requestIdTs = CantonTimestamp.Epoch
    val requestId = RequestId(requestIdTs)
    val decisionTime = requestIdTs.plusSeconds(120)

    val initialDomainParameters = TestDomainParameters.defaultDynamic

    val domainSyncCryptoApi: DomainSyncCryptoClient =
      if (testedProtocolVersion >= ProtocolVersion.v31) {
        val topology = TestingTopology(
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

        val identityFactory = TestingIdentityFactory(
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
          Set(
            MediatorGroup(
              MediatorGroupIndex.zero,
              NonEmpty.mk(Seq, mediatorId),
              Seq.empty,
              PositiveInt.one,
            )
          ),
        )

        val identityFactory = TestingIdentityFactory(
          topology,
          loggerFactory,
          dynamicDomainParameters = initialDomainParameters,
        )

        identityFactory.forOwnerAndDomain(mediatorId, domainId)
      }

    private val sequencerClientSend: TestSequencerClientSend = new TestSequencerClientSend

    def interceptedMessages: Seq[(Batch[DefaultOpenEnvelope], Option[AggregationRule])] =
      sequencerClientSend.requestsQueue.asScala.map { request =>
        (request.batch, request.aggregationRule)
      }.toSeq

    val verdictSender = new DefaultVerdictSender(
      sequencerClientSend,
      domainSyncCryptoApi,
      mediatorId,
      testedProtocolVersion,
      loggerFactory,
    )

    def sendApproval(): Future[Unit] = {
      verdictSender
        .sendResult(
          requestId,
          informeeMessage,
          Verdict.Approve(testedProtocolVersion),
          decisionTime,
        )
        .onShutdown(fail())
    }

    def sendReject(): Future[Unit] = {
      verdictSender
        .sendReject(
          requestId,
          Some(informeeMessage),
          Seq(rhmEnvelope),
          MediatorVerdict
            .MediatorReject(MalformedMessage.Reject("Test failure"))
            .toVerdict(testedProtocolVersion),
          decisionTime,
        )
        .failOnShutdown
    }
  }

}
