// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.data.EitherT
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
  Batch,
  MessageId,
  OpenEnvelope,
  Recipient,
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
  TestingTopology,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, ProtocolVersionChecksAsyncWordSpec}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class DefaultVerdictSenderTest
    extends AsyncWordSpec
    with ProtocolVersionChecksAsyncWordSpec
    with BaseTest {

  val activeMediator1 = MediatorId(UniqueIdentifier.tryCreate("mediator", "one"))
  val activeMediator2 = MediatorId(UniqueIdentifier.tryCreate("mediator", "two"))

  val mediatorGroup: MediatorGroup = MediatorGroup(
    index = NonNegativeInt.zero,
    active = activeMediator2,
    threshold = PositiveInt.tryCreate(2),
  )

  "DefaultVerdictSender" should {
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
      Recipients.cc(transactionMediatorRef.toRecipient, Recipient(participant)),
    )(testedProtocolVersion)

    val submitter = ExampleTransactionFactory.submitter
    val signatory = ExampleTransactionFactory.signatory
    val observer = ExampleTransactionFactory.observer

    val requestIdTs = CantonTimestamp.Epoch
    val requestId = RequestId(requestIdTs)
    val decisionTime = requestIdTs.plusSeconds(120)

    val initialDomainParameters = TestDomainParameters.defaultDynamic

    val domainSyncCryptoApi: DomainSyncCryptoClient = {
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
      Batch[DefaultOpenEnvelope]
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
            callback: SendCallback,
        )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncClientError, Unit] = {
          interceptedMessages.add(batch)
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
