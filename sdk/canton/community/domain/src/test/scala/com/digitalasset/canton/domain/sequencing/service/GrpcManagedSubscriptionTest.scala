// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.domain.sequencing.sequencer.errors.CreateSubscriptionError
import com.digitalasset.canton.sequencing.SequencerTestUtils.MockMessageContent
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.client.SequencerSubscription
import com.digitalasset.canton.sequencing.client.SequencerSubscriptionError.SequencedEventError
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  DomainId,
  ParticipantId,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.SerializableTraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext, SequencerCounter}
import io.grpc.stub.ServerCallStreamObserver
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}

class GrpcManagedSubscriptionTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
  private class Env {
    val sequencerSubscription = mock[SequencerSubscription[SequencedEventError]]
    val domainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("da::default"))
    var handler: Option[SerializedEventOrErrorHandler[SequencedEventError]] = None
    val member = ParticipantId(DefaultTestIdentities.uid)
    val observer = mock[ServerCallStreamObserver[v30.VersionedSubscriptionResponse]]
    var cancelCallback: Option[Runnable] = None

    when(observer.setOnCancelHandler(any[Runnable]))
      .thenAnswer[Runnable](handler => cancelCallback = Some(handler))

    def cancel(): Unit =
      cancelCallback.fold(fail("no cancel handler registered"))(_.run())

    def createSequencerSubscription(
        newHandler: SerializedEventOrErrorHandler[SequencedEventError]
    ): EitherT[Future, CreateSubscriptionError, SequencerSubscription[SequencedEventError]] = {
      handler = Some(newHandler)
      EitherT.rightT[Future, CreateSubscriptionError](sequencerSubscription)
    }

    def deliver(): Unit = {
      val message = MockMessageContent.toByteString
      val event = SignedContent(
        Deliver.create(
          SequencerCounter(0),
          CantonTimestamp.Epoch,
          domainId,
          Some(MessageId.tryCreate("test-deliver")),
          Batch(
            List(
              ClosedEnvelope
                .create(message, Recipients.cc(member), Seq.empty, testedProtocolVersion)
            ),
            testedProtocolVersion,
          ),
          None,
          testedProtocolVersion,
          Option.empty[TrafficReceipt],
        ),
        SymbolicCrypto.emptySignature,
        None,
        testedProtocolVersion,
      )
      handler.fold(fail("handler not registered"))(h =>
        Await.result(h(Right(OrdinarySequencedEvent(event)(traceContext))), 5.seconds)
      )
    }

    private def toSubscriptionResponseV30(event: OrdinarySerializedEvent) =
      v30.VersionedSubscriptionResponse(
        signedSequencedEvent = event.signedEvent.toByteString,
        Some(SerializableTraceContext(event.traceContext).toProtoV30),
      )

    def createManagedSubscription() =
      new GrpcManagedSubscription(
        createSequencerSubscription,
        observer,
        member,
        None,
        timeouts,
        loggerFactory,
        toSubscriptionResponseV30,
      )
  }

  "GrpcManagedSubscription" should {
    "send received events" in new Env {
      createManagedSubscription()
      deliver()
      verify(observer).onNext(any[v30.VersionedSubscriptionResponse])
    }

    "if observer is cancelled then subscription is closed but no response is sent" in new Env {
      createManagedSubscription()
      cancel()
      verify(sequencerSubscription).close()
      verify(observer, never).onError(any[Throwable])
      verify(observer, never).onCompleted()
    }

    "if closed externally the observer is completed, the subscription is closed, but the closed callback is not called" in new Env {
      val subscription = createManagedSubscription()
      subscription.close()
      verify(sequencerSubscription).close()
      verify(observer).onCompleted()
    }
  }
}
