// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.traffic

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.UnlessShutdown
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  SetTrafficPurchasedMessage,
  SignedProtocolMessage,
}
import com.digitalasset.canton.protocol.{DynamicSynchronizerParameters, SynchronizerParameters}
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.sequencing.client.{
  SendAsyncClientError,
  SendCallback,
  SendResult,
  SequencerClientSend,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.{
  TrafficControlErrors,
  TrafficPurchasedSubmissionHandler,
  TrafficReceipt,
}
import com.digitalasset.canton.time.{SimClock, SynchronizerTimeTracker}
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
import org.mockito.Mockito.clearInvocations
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

import java.time.{LocalDateTime, ZoneOffset}
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Try

class TrafficPurchasedSubmissionHandlerTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with BeforeAndAfterEach
    with ProtocolVersionChecksAnyWordSpec {

  private val recipient1 = DefaultTestIdentities.participant1.member
  private val sequencerClient = mock[SequencerClientSend]
  private val synchronizerTimeTracker = mock[SynchronizerTimeTracker]
  private val synchronizerId = SynchronizerId.tryFromString("da::default")
  private val clock = new SimClock(loggerFactory = loggerFactory)
  private val trafficParams = TrafficControlParameters()
  private val handler = new TrafficPurchasedSubmissionHandler(clock, loggerFactory)
  val crypto = TestingTopology(
    synchronizerParameters = List(
      SynchronizerParameters.WithValidity(
        validFrom = CantonTimestamp.Epoch.minusSeconds(1),
        validUntil = None,
        parameter = DynamicSynchronizerParameters
          .defaultValues(testedProtocolVersion)
          .tryUpdate(trafficControlParameters = Some(trafficParams)),
      )
    )
  ).build(loggerFactory)
    .forOwnerAndSynchronizer(DefaultTestIdentities.sequencerId, synchronizerId)

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
        any[Option[CantonTimestamp]],
        maxSequencingTimeCapture.capture(),
        any[MessageId],
        aggregationRuleCapture.capture(),
        callbackCapture.capture(),
        any[Boolean],
      )(any[TraceContext], any[MetricsContext])
    ).thenReturn(EitherT.pure(()))

    val resultF = handler
      .sendTrafficPurchasedRequest(
        recipient1,
        synchronizerId,
        testedProtocolVersion,
        PositiveInt.tryCreate(5),
        NonNegativeLong.tryCreate(1000),
        sequencerClient,
        synchronizerTimeTracker,
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

    resultF.failOnShutdown.futureValue shouldBe Either.unit

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
                SequencersOfSynchronizer: Recipient,
              )
            )
          ),
        ),
      )
    )
    batch.envelopes.foreach { envelope =>
      envelope.protocolMessage shouldBe a[SignedProtocolMessage[_]]
      val topUpMessage = envelope.protocolMessage
        .asInstanceOf[SignedProtocolMessage[SetTrafficPurchasedMessage]]
        .message
      topUpMessage.synchronizerId shouldBe synchronizerId
      topUpMessage.serial.value shouldBe 5
      topUpMessage.member shouldBe recipient1
      topUpMessage.totalTrafficPurchased.value shouldBe 1000
    }
  }

  "send 2 messages if close to the end of the max sequencing time window" in {
    val callbackCapture: ArgumentCaptor[SendCallback] =
      ArgumentCaptor.forClass(classOf[SendCallback])
    val maxSequencingTimeCapture: ArgumentCaptor[CantonTimestamp] =
      ArgumentCaptor.forClass(classOf[CantonTimestamp])

    val minutesBucketEnd =
      (8 * trafficParams.setBalanceRequestSubmissionWindowSize.duration.toMinutes).toInt
    // 01/01/2024 15:31:00
    val currentSimTime = LocalDateTime.of(2024, 1, 1, 15, minutesBucketEnd - 1, 0)
    val newTime = CantonTimestamp.ofEpochMilli(
      currentSimTime.toInstant(ZoneOffset.UTC).toEpochMilli
    )
    // Advance the clock to 15:minutesBucketEnd - 1 - within one minute of the next time bucket (every setBalanceRequestSubmissionWindowSize minutes)
    clock.advanceTo(newTime)

    when(
      sequencerClient.sendAsync(
        any[Batch[DefaultOpenEnvelope]],
        any[Option[CantonTimestamp]],
        maxSequencingTimeCapture.capture(),
        any[MessageId],
        any[Option[AggregationRule]],
        callbackCapture.capture(),
        any[Boolean],
      )(any[TraceContext], any[MetricsContext])
    ).thenReturn(EitherT.pure(()))

    val resultF = handler
      .sendTrafficPurchasedRequest(
        recipient1,
        synchronizerId,
        testedProtocolVersion,
        PositiveInt.tryCreate(5),
        NonNegativeLong.tryCreate(1000),
        sequencerClient,
        synchronizerTimeTracker,
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
      mkTimeBucketUpperBound(minutesBucketEnd),
      mkTimeBucketUpperBound(
        minutesBucketEnd + trafficParams.setBalanceRequestSubmissionWindowSize.duration.toMinutes.toInt
      ),
    )

    resultF.failOnShutdown.futureValue shouldBe Either.unit
  }

  "catch sequencer client failures" in {
    when(
      sequencerClient.sendAsync(
        any[Batch[DefaultOpenEnvelope]],
        any[Option[CantonTimestamp]],
        any[CantonTimestamp],
        any[MessageId],
        any[Option[AggregationRule]],
        any[SendCallback],
        any[Boolean],
      )(any[TraceContext], any[MetricsContext])
    )
      .thenReturn(EitherT.leftT(SendAsyncClientError.RequestFailed("failed")))

    handler
      .sendTrafficPurchasedRequest(
        recipient1,
        synchronizerId,
        testedProtocolVersion,
        PositiveInt.tryCreate(5),
        NonNegativeLong.tryCreate(1000),
        sequencerClient,
        synchronizerTimeTracker,
        crypto,
      )
      .value
      .failOnShutdown
      .futureValue shouldBe Left(
      TrafficControlErrors.TrafficPurchasedRequestAsyncSendFailed.Error(
        "RequestFailed(failed)"
      )
    )
  }

  "log sequencing failures" in {
    val callbackCapture: ArgumentCaptor[SendCallback] =
      ArgumentCaptor.forClass(classOf[SendCallback])
    when(
      sequencerClient.sendAsync(
        any[Batch[DefaultOpenEnvelope]],
        any[Option[CantonTimestamp]],
        any[CantonTimestamp],
        any[MessageId],
        any[Option[AggregationRule]],
        callbackCapture.capture(),
        any[Boolean],
      )(any[TraceContext], any[MetricsContext])
    )
      .thenReturn(EitherT.pure(()))

    val messageId = MessageId.randomMessageId()
    val deliverError = DeliverError.create(
      SequencerCounter.Genesis,
      CantonTimestamp.Epoch,
      synchronizerId,
      messageId,
      Status.defaultInstance.withMessage("BOOM"),
      testedProtocolVersion,
      Option.empty[TrafficReceipt],
    )

    loggerFactory.assertEventuallyLogsSeq(SuppressionRule.Level(Level.INFO))(
      {
        val resultF = handler.sendTrafficPurchasedRequest(
          recipient1,
          synchronizerId,
          testedProtocolVersion,
          PositiveInt.tryCreate(5),
          NonNegativeLong.tryCreate(1000),
          sequencerClient,
          synchronizerTimeTracker,
          crypto,
        )

        eventually() {
          Try(callbackCapture.getValue).isSuccess shouldBe true
        }
        callbackCapture.getValue.asInstanceOf[SendCallback.CallbackFuture](
          UnlessShutdown.Outcome(SendResult.Error(deliverError))
        )

        resultF.failOnShutdown.value.futureValue shouldBe Either.unit
      },
      LogEntry.assertLogSeq(
        Seq(
          (
            _.message should include(
              s"The traffic balance request submission failed: DeliverError(counter = 0, timestamp = 1970-01-01T00:00:00Z, synchronizer id = da::default, message id = $messageId, reason = Status(OK, BOOM))"
            ),
            "sequencing failure",
          )
        ),
        Seq(_ => succeed),
      ),
    )
  }

  "log sequencing timeouts" in {
    val callbackCapture: ArgumentCaptor[SendCallback] =
      ArgumentCaptor.forClass(classOf[SendCallback])
    when(
      sequencerClient.sendAsync(
        any[Batch[DefaultOpenEnvelope]],
        any[Option[CantonTimestamp]],
        any[CantonTimestamp],
        any[MessageId],
        any[Option[AggregationRule]],
        callbackCapture.capture(),
        any[Boolean],
      )(any[TraceContext], any[MetricsContext])
    )
      .thenReturn(EitherT.pure(()))
    clearInvocations(synchronizerTimeTracker)

    loggerFactory.assertEventuallyLogsSeq(SuppressionRule.Level(Level.WARN))(
      {
        val resultF = handler.sendTrafficPurchasedRequest(
          recipient1,
          synchronizerId,
          testedProtocolVersion,
          PositiveInt.tryCreate(5),
          NonNegativeLong.tryCreate(1000),
          sequencerClient,
          synchronizerTimeTracker,
          crypto,
        )

        eventually() {
          Try(callbackCapture.getValue).isSuccess shouldBe true
        }
        callbackCapture.getValue.asInstanceOf[SendCallback.CallbackFuture](
          UnlessShutdown.Outcome(SendResult.Timeout(CantonTimestamp.Epoch))
        )

        resultF.value.failOnShutdown.futureValue shouldBe Either.unit
      },
      LogEntry.assertLogSeq(
        Seq(
          (
            _.warningMessage should include(
              s"The traffic balance request submission timed out after sequencing time 1970-01-01T00:00:00Z has elapsed"
            ),
            "timeout",
          )
        ),
        Seq.empty,
      ),
    )

    // Check that a tick was requested so that the sequencer will actually observe the timeout
    verify(synchronizerTimeTracker).requestTick(any[CantonTimestamp], any[Boolean])(
      any[TraceContext]
    )
  }
}
