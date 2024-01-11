// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports

import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.crypto.v0 as cryptoproto
import com.digitalasset.canton.domain.api.v0 as v0domain
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.sequencing.SequencerTestUtils.MockMessageContent
import com.digitalasset.canton.sequencing.client.SubscriptionCloseReason
import com.digitalasset.canton.topology.{DomainId, UniqueIdentifier}
import com.digitalasset.canton.tracing.SerializableTraceContext
import com.digitalasset.canton.util.ByteStringUtil
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp
import io.grpc.Context.CancellableContext
import io.grpc.Status.Code.*
import io.grpc.{Context, Status, StatusRuntimeException}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Future, Promise}

class GrpcSequencerSubscriptionTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  val domainId: DomainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("da::default"))

  val MessageP: v0domain.SubscriptionResponse = v0domain
    .SubscriptionResponse(
      Some(
        v0.SignedContent(
          Some(
            v0.SequencedEvent(
              timestamp = Some(Timestamp()),
              batch = Some(
                v0.CompressedBatch(
                  algorithm = v0.CompressedBatch.CompressionAlgorithm.None,
                  compressedBatch = ByteStringUtil.compressGzip(
                    v0.Batch(envelopes =
                      Seq(v0.Envelope(content = MockMessageContent.toByteString, recipients = None))
                    ).toByteString
                  ),
                )
              ),
              domainId = domainId.toProtoPrimitive,
              counter = 0L,
              messageId = None,
              deliverErrorReason = None,
            ).toByteString
          ),
          Some(
            cryptoproto.Signature(
              format = cryptoproto.SignatureFormat.RawSignatureFormat,
              signature = ByteString.copyFromUtf8("not checked in this test"),
              signedBy = "not checked",
            )
          ),
          timestampOfSigningKey = None,
        )
      ),
      Some(SerializableTraceContext.empty.toProtoV0),
    )

  val RequestDescription = "request description"

  val ServerName = "sequencer"

  def expectedError(ex: StatusRuntimeException) =
    Left(GrpcError(RequestDescription, ServerName, ex))

  def createSubscription(
      handler: v0domain.SubscriptionResponse => Future[Either[String, Unit]] = _ =>
        Future.successful(Right(())),
      context: CancellableContext = Context.ROOT.withCancellation(),
  ): GrpcSequencerSubscription[String, v0domain.SubscriptionResponse] =
    new GrpcSequencerSubscription[String, v0domain.SubscriptionResponse](
      context,
      tracedEvent => handler(tracedEvent.value), // ignore Traced[..] wrapper
      CommonMockMetrics.sequencerClient,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
    ) {
      // reduce the close timeout
      override def maxSleepMillis: Long = 10
      override def closingTimeout: FiniteDuration = 1.second
    }

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
      val messagePromise = Promise[v0domain.SubscriptionResponse]()

      val sut =
        createSubscription(handler = m => Future.successful(Right(messagePromise.success(m))))

      sut.observer.onNext(MessageP)

      messagePromise.future.futureValue shouldBe MessageP
    }

    "close with exception if the handler throws" in {
      val ex = new RuntimeException("Handler Error")
      val sut = createSubscription(handler = _ => Future.failed(ex))

      sut.observer.onNext(MessageP)

      sut.closeReason.futureValue shouldBe SubscriptionCloseReason.HandlerException(ex)
    }

    "terminate onNext only after termination of the handler" in {
      val handlerCompleted = Promise[Either[String, Unit]]()

      val sut = createSubscription(handler = _ => handlerCompleted.future)

      val onNextF = Future { sut.observer.onNext(MessageP) }

      eventuallyForever(timeUntilSuccess = 0.seconds, durationOfSuccess = 100.milliseconds) {
        !onNextF.isCompleted
      }

      handlerCompleted.success(Right(()))

      onNextF.futureValue
    }

    "not wait for the handler to complete on shutdown" in {
      val handlerInvoked = Promise[Unit]()
      val handlerCompleted = Promise[Either[String, Unit]]()

      val sut = createSubscription(handler = _ => {
        handlerInvoked.success(())
        handlerCompleted.future
      })

      // Processing this message takes forever...
      Future { sut.observer.onNext(MessageP) }.failed
        .foreach(logger.error("Unexpected exception", _))

      // Make sure that the handler has been invoked before doing the next step.
      handlerInvoked.future.futureValue

      sut.close()
      sut.closeReason.futureValue shouldBe SubscriptionCloseReason.Shutdown
    }

    "not invoke the handler after closing" in {
      val messagePromise = Promise[v0domain.SubscriptionResponse]()

      val sut =
        createSubscription(handler = m => Future.successful(Right(messagePromise.success(m))))

      sut.close()

      sut.observer.onNext(MessageP)

      eventuallyForever(timeUntilSuccess = 0.seconds, durationOfSuccess = 100.milliseconds) {
        !messagePromise.isCompleted
      }
    }

    "not log a INTERNAL error at error level after having received some items" in {
      // we see this scenario when a load balancer between applications decides to reset the TCP stream, say for a timeout
      val sut = createSubscription(handler = _ => Future.successful(Right(())))

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          // receive some items
          sut.observer.onNext(
            v0domain.SubscriptionResponse.defaultInstance
              .copy(traceContext = Some(SerializableTraceContext.empty.toProtoV0))
          )
          sut.observer.onError(Status.INTERNAL.asRuntimeException())
          sut.close()
        },
        { logs =>
          logs shouldBe empty
        },
      )
    }
  }
}
