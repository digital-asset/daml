// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.traffic

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.UnlessShutdown
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  SetTrafficBalanceMessage,
  SignedProtocolMessage,
}
import com.digitalasset.canton.protocol.{DomainParameters, DynamicDomainParameters}
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.sequencing.client.{
  SendAsyncClientError,
  SendCallback,
  SendResult,
  SendType,
  SequencerClientSend,
}
import com.digitalasset.canton.sequencing.protocol.{SequencersOfDomain, *}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  ProtocolVersionChecksAnyWordSpec,
  SequencerCounter,
}
import com.google.rpc.status.Status
import org.mockito.ArgumentCaptor
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpec

import java.time.{LocalDateTime, ZoneOffset}
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Try

class TrafficBalanceSubmissionHandlerTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with BeforeAndAfterEach
    with ProtocolVersionChecksAnyWordSpec {

  private val recipient1 = DefaultTestIdentities.participant1.member
  private val sequencerClient = mock[SequencerClientSend]
  private val domainId = DomainId.tryFromString("da::default")
  private val clock = new SimClock(loggerFactory = loggerFactory)
  private val trafficParams = TrafficControlParameters()
  private val handler = new TrafficBalanceSubmissionHandler(clock, loggerFactory)
  val crypto = new TestingIdentityFactoryX(
    TestingTopologyX(),
    loggerFactory,
    dynamicDomainParameters = List(
      DomainParameters.WithValidity(
        validFrom = CantonTimestamp.Epoch.minusSeconds(1),
        validUntil = None,
        parameter = DynamicDomainParameters
          .defaultValues(testedProtocolVersion)
          .tryUpdate(trafficControlParameters = Some(trafficParams)),
      )
    ),
  )
    .forOwnerAndDomain(DefaultTestIdentities.sequencerIdX, domainId)

  override def beforeEach(): Unit = {
    super.beforeEach()
    clock.reset()
  }

  "send a well formed top up message" in {
    val maxSequencingTimeCapture: ArgumentCaptor[CantonTimestamp] =
      ArgumentCaptor.forClass(classOf[CantonTimestamp])
    val batchCapture: ArgumentCaptor[Batch[DefaultOpenEnvelope]] =
      ArgumentCaptor.forClass(classOf[Batch[DefaultOpenEnvelope]])
    val aggregationRuleCapture = ArgumentCaptor.forClass(classOf[Option[AggregationRule]])
    val callbackCapture: ArgumentCaptor[SendCallback] =
      ArgumentCaptor.forClass(classOf[SendCallback])
    when(
      sequencerClient.sendAsync(
        batchCapture.capture(),
        any[SendType],
        any[Option[CantonTimestamp]],
        maxSequencingTimeCapture.capture(),
        any[MessageId],
        aggregationRuleCapture.capture(),
        callbackCapture.capture(),
        any[Boolean],
      )(any[TraceContext])
    ).thenReturn(EitherT.pure(()))

    val resultF = handler
      .sendTrafficBalanceRequest(
        recipient1,
        domainId,
        testedProtocolVersion,
        PositiveInt.tryCreate(5),
        NonNegativeLong.tryCreate(1000),
        sequencerClient,
        crypto,
      )
      .value

    eventually() {
      Try(callbackCapture.getValue).isSuccess shouldBe true
    }
    callbackCapture.getValue.asInstanceOf[SendCallback.CallbackFuture](
      UnlessShutdown.Outcome(SendResult.Success(mock[Deliver[Envelope[_]]]))
    )
    maxSequencingTimeCapture.getValue shouldBe clock.now.plusSeconds(
      trafficParams.setBalanceRequestSubmissionWindowSize.duration.toSeconds
    )

    resultF.failOnShutdown.futureValue shouldBe Right(
      clock.now.plusSeconds(trafficParams.setBalanceRequestSubmissionWindowSize.duration.toSeconds)
    )

    val batch = batchCapture.getValue
    batch.envelopes.head.recipients shouldBe Recipients(
      NonEmpty.mk(
        Seq,
        RecipientsTree.ofMembers(
          NonEmpty.mk(Set, recipient1), // Root of recipient tree: recipient of the top up
          Seq(
            RecipientsTree.recipientsLeaf( // Leaf of the tree: sequencers of domain group
              NonEmpty.mk(
                Set,
                SequencersOfDomain: Recipient,
              )
            )
          ),
        ),
      )
    )
    batch.envelopes.foreach { envelope =>
      envelope.protocolMessage shouldBe a[SignedProtocolMessage[_]]
      val topUpMessage = envelope.protocolMessage
        .asInstanceOf[SignedProtocolMessage[SetTrafficBalanceMessage]]
        .message
      topUpMessage.domainId shouldBe domainId
      topUpMessage.serial.value shouldBe 5
      topUpMessage.member shouldBe recipient1
      topUpMessage.totalTrafficBalance.value shouldBe 1000
    }
  }

  "send 2 messages if close to the end of the max sequencing time window" in {
    val callbackCapture: ArgumentCaptor[SendCallback] =
      ArgumentCaptor.forClass(classOf[SendCallback])
    val maxSequencingTimeCapture: ArgumentCaptor[CantonTimestamp] =
      ArgumentCaptor.forClass(classOf[CantonTimestamp])

    // 01/01/2024 15:39:00
    val currentSimTime = LocalDateTime.of(2024, 1, 1, 15, 39, 0)
    val newTime = CantonTimestamp.ofEpochMilli(
      currentSimTime.toInstant(ZoneOffset.UTC).toEpochMilli
    )
    // Advance the clock to 15:39 - within one minute of the next time bucket
    clock.advanceTo(newTime)

    when(
      sequencerClient.sendAsync(
        any[Batch[DefaultOpenEnvelope]],
        any[SendType],
        any[Option[CantonTimestamp]],
        maxSequencingTimeCapture.capture(),
        any[MessageId],
        any[Option[AggregationRule]],
        callbackCapture.capture(),
        any[Boolean],
      )(any[TraceContext])
    ).thenReturn(EitherT.pure(()))

    val resultF = handler
      .sendTrafficBalanceRequest(
        recipient1,
        domainId,
        testedProtocolVersion,
        PositiveInt.tryCreate(5),
        NonNegativeLong.tryCreate(1000),
        sequencerClient,
        crypto,
      )
      .value

    eventually() {
      Try(callbackCapture.getAllValues).isSuccess shouldBe true
      Try(maxSequencingTimeCapture.getAllValues).isSuccess shouldBe true
      callbackCapture.getAllValues.size() shouldBe 2
      maxSequencingTimeCapture.getAllValues.size() shouldBe 2
    }
    callbackCapture.getAllValues.asScala.foreach {
      _.asInstanceOf[SendCallback.CallbackFuture](
        UnlessShutdown.Outcome(SendResult.Success(mock[Deliver[Envelope[_]]]))
      )
    }

    def mkTimeBucketUpperBound(minutes: Int) = CantonTimestamp.ofEpochMilli(
      currentSimTime
        .withMinute(minutes)
        .toInstant(ZoneOffset.UTC)
        .toEpochMilli
    )

    maxSequencingTimeCapture.getAllValues.asScala should contain theSameElementsAs List(
      mkTimeBucketUpperBound(40),
      mkTimeBucketUpperBound(45),
    )

    resultF.failOnShutdown.futureValue shouldBe Right(mkTimeBucketUpperBound(45))
  }

  "catch sequencer client failures" in {
    when(
      sequencerClient.sendAsync(
        any[Batch[DefaultOpenEnvelope]],
        any[SendType],
        any[Option[CantonTimestamp]],
        any[CantonTimestamp],
        any[MessageId],
        any[Option[AggregationRule]],
        any[SendCallback],
        any[Boolean],
      )(any[TraceContext])
    )
      .thenReturn(EitherT.leftT(SendAsyncClientError.RequestFailed("failed")))

    handler
      .sendTrafficBalanceRequest(
        recipient1,
        domainId,
        testedProtocolVersion,
        PositiveInt.tryCreate(5),
        NonNegativeLong.tryCreate(1000),
        sequencerClient,
        crypto,
      )
      .value
      .failOnShutdown
      .futureValue shouldBe Left(
      TrafficControlErrors.TrafficBalanceRequestAsyncSendFailed.Error(
        "RequestFailed(failed)"
      )
    )
  }

  "catch sequencing failures" in {
    val callbackCapture: ArgumentCaptor[SendCallback] =
      ArgumentCaptor.forClass(classOf[SendCallback])
    when(
      sequencerClient.sendAsync(
        any[Batch[DefaultOpenEnvelope]],
        any[SendType],
        any[Option[CantonTimestamp]],
        any[CantonTimestamp],
        any[MessageId],
        any[Option[AggregationRule]],
        callbackCapture.capture(),
        any[Boolean],
      )(any[TraceContext])
    )
      .thenReturn(EitherT.pure(()))

    val resultF = handler
      .sendTrafficBalanceRequest(
        recipient1,
        domainId,
        testedProtocolVersion,
        PositiveInt.tryCreate(5),
        NonNegativeLong.tryCreate(1000),
        sequencerClient,
        crypto,
      )
      .value

    eventually() {
      Try(callbackCapture.getValue).isSuccess shouldBe true
    }
    val messageId = MessageId.randomMessageId()
    val deliverError = DeliverError.create(
      SequencerCounter.Genesis,
      CantonTimestamp.Epoch,
      domainId,
      messageId,
      Status.defaultInstance.withMessage("BOOM"),
      testedProtocolVersion,
    )
    callbackCapture.getValue.asInstanceOf[SendCallback.CallbackFuture](
      UnlessShutdown.Outcome(SendResult.Error(deliverError))
    )

    resultF.failOnShutdown.futureValue shouldBe Left(
      TrafficControlErrors.TrafficBalanceRequestAsyncSendFailed.Error(
        s"DeliverError(counter = 0, timestamp = 1970-01-01T00:00:00Z, domain id = da::default, message id = $messageId, reason = Status(OK, BOOM))"
      )
    )
  }

  "catch sequencing timeouts" in {
    val callbackCapture: ArgumentCaptor[SendCallback] =
      ArgumentCaptor.forClass(classOf[SendCallback])
    when(
      sequencerClient.sendAsync(
        any[Batch[DefaultOpenEnvelope]],
        any[SendType],
        any[Option[CantonTimestamp]],
        any[CantonTimestamp],
        any[MessageId],
        any[Option[AggregationRule]],
        callbackCapture.capture(),
        any[Boolean],
      )(any[TraceContext])
    )
      .thenReturn(EitherT.pure(()))

    val resultF = handler
      .sendTrafficBalanceRequest(
        recipient1,
        domainId,
        testedProtocolVersion,
        PositiveInt.tryCreate(5),
        NonNegativeLong.tryCreate(1000),
        sequencerClient,
        crypto,
      )
      .value

    eventually() {
      Try(callbackCapture.getValue).isSuccess shouldBe true
    }
    callbackCapture.getValue.asInstanceOf[SendCallback.CallbackFuture](
      UnlessShutdown.Outcome(SendResult.Timeout(CantonTimestamp.Epoch))
    )

    resultF.failOnShutdown.futureValue shouldBe Left(
      TrafficControlErrors.TrafficBalanceRequestAsyncSendFailed.Error(
        s"Submission timed out after sequencing time ${CantonTimestamp.Epoch} has elapsed"
      )
    )
  }
}
