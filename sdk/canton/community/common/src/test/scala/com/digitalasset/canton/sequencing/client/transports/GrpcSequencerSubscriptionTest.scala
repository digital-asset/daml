// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.crypto.v30 as cryptoproto
import com.digitalasset.canton.lifecycle.OnShutdownRunner.PureOnShutdownRunner
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.sequencer.api.v30 as v30Sequencer
import com.digitalasset.canton.sequencing.SequencerTestUtils.MockMessageContent
import com.digitalasset.canton.sequencing.client.SubscriptionCloseReason
import com.digitalasset.canton.topology.{SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.SerializableTraceContext
import com.digitalasset.canton.util.ByteStringUtil
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.google.protobuf.ByteString
import io.grpc.Context.CancellableContext
import io.grpc.Status.Code.*
import io.grpc.{Context, Status, StatusRuntimeException}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Future, Promise}

class GrpcSequencerSubscriptionTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  private lazy val synchronizerId: SynchronizerId = SynchronizerId(
    UniqueIdentifier.tryFromProtoPrimitive("da::default")
  )

  private lazy val emptyEnvelope = v30.Envelope(
    content = MockMessageContent.toByteString,
    recipients = None,
    signatures = Nil,
  )

  private lazy val messageP: v30Sequencer.VersionedSubscriptionResponse = v30Sequencer
    .VersionedSubscriptionResponse(
      v30
        .SignedContent(
          Some(
            v30
              .SequencedEvent(
                timestamp = 0,
                batch = Some(
                  v30.CompressedBatch(
                    algorithm =
                      v30.CompressedBatch.CompressionAlgorithm.COMPRESSION_ALGORITHM_UNSPECIFIED,
                    compressedBatch = ByteStringUtil.compressGzip(
                      v30.Batch(envelopes = Seq(emptyEnvelope)).toByteString
                    ),
                  )
                ),
                synchronizerId = synchronizerId.toProtoPrimitive,
                counter = 0L,
                messageId = None,
                deliverErrorReason = None,
                topologyTimestamp = None,
                trafficReceipt = None,
              )
              .toByteString
          ),
          Seq(
            cryptoproto.Signature(
              format = cryptoproto.SignatureFormat.SIGNATURE_FORMAT_RAW,
              signature = ByteString.copyFromUtf8("not checked in this test"),
              signedBy = "not checked",
              signingAlgorithmSpec =
                cryptoproto.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_UNSPECIFIED,
              signatureDelegation = None,
            )
          ),
          timestampOfSigningKey = None,
        )
        .toByteString,
      Some(SerializableTraceContext.empty.toProtoV30),
    )

  val RequestDescription = "request description"

  val ServerName = "sequencer"

  def expectedError(ex: StatusRuntimeException): Left[GrpcError, Nothing] =
    Left(GrpcError(RequestDescription, ServerName, ex))

  def createSubscription(
      handler: v30Sequencer.VersionedSubscriptionResponse => EitherT[
        FutureUnlessShutdown,
        String,
        Unit,
      ] = _ => handlerResult(Either.unit),
      context: CancellableContext = Context.ROOT.withCancellation(),
  ): GrpcSequencerSubscription[String, v30Sequencer.VersionedSubscriptionResponse] =
    new GrpcSequencerSubscription[String, v30Sequencer.VersionedSubscriptionResponse](
      context,
      new PureOnShutdownRunner(logger),
      tracedEvent => handler(tracedEvent.value), // ignore Traced[..] wrapper
      DefaultProcessingTimeouts.testing,
      loggerFactory,
    ) {
      // reduce the close timeout
      override def maxSleepMillis: Long = 10
      override def closingTimeout: FiniteDuration = 1.second
    }

  private def handlerResult(
      either: Either[String, Unit]
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    EitherT(FutureUnlessShutdown.pure(either))

  "GrpcSequencerSubscription" should {
    "close normally when closed by the user" in {
      val context = Context.ROOT.withCancellation()
      val sut = createSubscription(context = context)

      // The user closes the observer
      sut.close()

      // This must close the context
      context.isCancelled shouldBe true

      sut.closeReason.futureValue shouldBe SubscriptionCloseReason.Closed
    }

    "pass any exception given to onError" in {
      val ex = new RuntimeException("Test exception")

      val sut = createSubscription()

      loggerFactory.assertLogs(
        sut.observer.onError(ex),
        entry => {
          entry.errorMessage shouldBe "The sequencer subscription failed unexpectedly."
          entry.throwable shouldBe Some(ex)
        },
      )

      sut.closeReason.futureValue shouldBe GrpcSubscriptionUnexpectedException(ex)
    }

    "close with error when closed by the server" in {
      val sut = createSubscription()

      sut.observer.onCompleted()

      inside(sut.closeReason.futureValue) {
        case GrpcSubscriptionError(GrpcError.GrpcServiceUnavailable(_, _, status, _, _)) =>
          status.getCode shouldBe UNAVAILABLE
          status.getDescription shouldBe "Connection terminated by the server."
      }
    }

    "use the given handler to process received messages" in {
      val messagePromise = Promise[v30Sequencer.VersionedSubscriptionResponse]()

      val sut =
        createSubscription(handler = m => handlerResult(Right(messagePromise.success(m))))

      sut.observer.onNext(messageP)

      messagePromise.future.futureValue shouldBe messageP
    }

    "close with exception if the handler throws" in {
      val ex = new RuntimeException("Handler Error")
      val sut = createSubscription(handler =
        _ => EitherT(FutureUnlessShutdown.failed[Either[String, Unit]](ex))
      )

      sut.observer.onNext(messageP)

      sut.closeReason.futureValue shouldBe SubscriptionCloseReason.HandlerException(ex)
    }

    "terminate onNext only after termination of the handler" in {
      val handlerCompleted = Promise[UnlessShutdown[Either[String, Unit]]]()

      val sut =
        createSubscription(handler = _ => EitherT(FutureUnlessShutdown(handlerCompleted.future)))

      val onNextF = Future(sut.observer.onNext(messageP))

      eventuallyForever(timeUntilSuccess = 0.seconds, durationOfSuccess = 100.milliseconds) {
        !onNextF.isCompleted
      }

      handlerCompleted.success(UnlessShutdown.Outcome(Either.unit))

      onNextF.futureValue
    }

    "not wait for the handler to complete on shutdown" in {
      val handlerInvoked = Promise[Unit]()
      val handlerNeverCompleted = EitherT(
        FutureUnlessShutdown(Promise[UnlessShutdown.Outcome[Either[String, Unit]]]().future)
      )

      val sut = createSubscription(handler = _ => {
        handlerInvoked.success(())
        handlerNeverCompleted
      })

      // Processing this message takes forever...
      Future(sut.observer.onNext(messageP)).failed
        .foreach(logger.error("Unexpected exception", _))

      // Make sure that the handler has been invoked before doing the next step.
      handlerInvoked.future.futureValue

      sut.close()
      sut.closeReason.futureValue shouldBe SubscriptionCloseReason.Shutdown
    }

    "not invoke the handler after closing" in {
      val messagePromise = Promise[v30Sequencer.VersionedSubscriptionResponse]()

      val sut =
        createSubscription(handler = m => handlerResult(Right(messagePromise.success(m))))

      sut.close()

      sut.observer.onNext(messageP)

      eventuallyForever(timeUntilSuccess = 0.seconds, durationOfSuccess = 100.milliseconds) {
        !messagePromise.isCompleted
      }
    }

    "not log a INTERNAL error at error level after having received some items" in {
      // we see this scenario when a load balancer between applications decides to reset the TCP stream, say for a timeout
      val sut =
        createSubscription(handler = _ => handlerResult(Either.unit))

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          // receive some items
          sut.observer.onNext(
            v30Sequencer.VersionedSubscriptionResponse.defaultInstance
              .copy(traceContext = Some(SerializableTraceContext.empty.toProtoV30))
          )
          sut.observer.onError(Status.INTERNAL.asRuntimeException())
          sut.close()
        },
        logs => logs shouldBe empty,
      )
    }
  }
}
