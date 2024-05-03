// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.sequencing.sequencer.errors.CreateSubscriptionError
import com.digitalasset.canton.domain.sequencing.service.DirectSequencerSubscriptionFactory
import com.digitalasset.canton.health.{AtomicHealthComponent, ComponentHealthState}
import com.digitalasset.canton.lifecycle.{OnShutdownRunner, SyncCloseable}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.sequencing.SerializedEventHandler
import com.digitalasset.canton.sequencing.client.SendAsyncClientError.SendAsyncClientResponseError
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.sequencing.client.transports.{
  SequencerClientTransport,
  SequencerClientTransportPekko,
}
import com.digitalasset.canton.sequencing.handshake.HandshakeRequestError
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  HandshakeRequest,
  HandshakeResponse,
  SignedContent,
  SubmissionRequest,
  SubscriptionRequest,
  TopologyStateForInitRequest,
  TopologyStateForInitResponse,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil.DelayedKillSwitch
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{ErrorUtil, FutureUtil, PekkoUtil}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.{Done, NotUsed}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** This transport is meant to be used to create a sequencer client that connects directly to an in-process sequencer.
  * Needed for cases when the sequencer node itself needs to listen to specific events such as identity events.
  */
class DirectSequencerClientTransport(
    sequencer: Sequencer,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit materializer: Materializer, ec: ExecutionContext)
    extends SequencerClientTransport
    with SequencerClientTransportPekko
    with NamedLogging {
  import DirectSequencerClientTransport.*

  private val subscriptionFactory =
    new DirectSequencerSubscriptionFactory(sequencer, timeouts, loggerFactory)

  override def sendAsyncSigned(
      request: SignedContent[SubmissionRequest],
      timeout: Duration,
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncClientResponseError, Unit] =
    sequencer
      .sendAsyncSigned(request)
      .leftMap(SendAsyncClientError.RequestRefused)

  override def sendAsyncUnauthenticatedVersioned(
      request: SubmissionRequest,
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncClientResponseError, Unit] =
    ErrorUtil.internalError(
      new UnsupportedOperationException("Direct client does not support unauthenticated sends")
    )

  override def acknowledgeSigned(request: SignedContent[AcknowledgeRequest])(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Boolean] =
    sequencer.acknowledgeSigned(request).map { _ => true }

  override def subscribe[E](request: SubscriptionRequest, handler: SerializedEventHandler[E])(
      implicit traceContext: TraceContext
  ): SequencerSubscription[E] = new SequencerSubscription[E] {

    override protected def timeouts: ProcessingTimeout =
      DirectSequencerClientTransport.this.timeouts

    override protected val loggerFactory: NamedLoggerFactory =
      DirectSequencerClientTransport.this.loggerFactory

    private val subscriptionRef = new AtomicReference[Option[SequencerSubscription[E]]](None)

    {
      val subscriptionET =
        subscriptionFactory
          .create(
            request.counter,
            request.member,
            {
              case Right(event) => handler(event)
              case Left(error) =>
                ErrorUtil.invalidState(
                  s"Direct transport subscriptions must not trigger subscription errors such as ${error}"
                )
            },
          )
          .thereafter {
            case Success(Right(subscription)) =>
              closeReasonPromise.completeWith(subscription.closeReason)

              performUnlessClosing(functionFullName) {
                subscriptionRef.set(Some(subscription))
              } onShutdown {
                subscription.close()
              }
            case Success(Left(value)) =>
              closeReasonPromise.trySuccess(Fatal(value.toString)).discard[Boolean]
            case Failure(exception) =>
              closeReasonPromise.tryFailure(exception).discard[Boolean]
          }
      FutureUtil.doNotAwait(
        subscriptionET.value,
        s"creating the direct sequencer subscription for $request",
      )
    }

    override protected def closeAsync(): Seq[SyncCloseable] = Seq(
      SyncCloseable(
        "direct-sequencer-client-transport",
        subscriptionRef.get().foreach(_.close()),
      )
    )

    override private[canton] def complete(
        reason: SubscriptionCloseReason[E]
    )(implicit traceContext: TraceContext): Unit = {
      subscriptionRef.get().foreach(_.complete(reason))
      close()
    }
  }

  override def subscribeUnauthenticated[E](
      request: SubscriptionRequest,
      handler: SerializedEventHandler[E],
  )(implicit traceContext: TraceContext): SequencerSubscription[E] =
    unsupportedUnauthenticatedSubscription

  private def unsupportedUnauthenticatedSubscription(implicit traceContext: TraceContext): Nothing =
    ErrorUtil.internalError(
      new UnsupportedOperationException(
        "Direct client does not support unauthenticated subscriptions"
      )
    )

  override def subscriptionRetryPolicy: SubscriptionErrorRetryPolicy =
    // unlikely there will be any errors with this direct transport implementation
    SubscriptionErrorRetryPolicy.never

  override type SubscriptionError = DirectSequencerClientTransport.SubscriptionError

  override def subscribe(request: SubscriptionRequest)(implicit
      traceContext: TraceContext
  ): SequencerSubscriptionPekko[SubscriptionError] = {
    val sourceF = sequencer
      .read(request.member, request.counter)
      .value
      .map {
        case Left(creationError) =>
          Source
            .single(Left(SubscriptionCreationError(creationError)))
            .mapMaterializedValue((_: NotUsed) =>
              (PekkoUtil.noOpKillSwitch, Future.successful(Done))
            )
        case Right(source) => source.map(_.leftMap(SequencedEventError))
      }
    val health = new DirectSequencerClientTransportHealth(logger)
    val source = Source
      .futureSource(sourceF)
      .watchTermination() { (matF, terminationF) =>
        val directExecutionContext = DirectExecutionContext(noTracingLogger)
        val killSwitchF = matF.map { case (killSwitch, _) => killSwitch }(directExecutionContext)
        val killSwitch = new DelayedKillSwitch(killSwitchF, noTracingLogger)
        val doneF = matF
          .flatMap { case (_, doneF) => doneF }(directExecutionContext)
          .flatMap(_ => terminationF)(directExecutionContext)
          .thereafter { _ =>
            logger.debug("Closing direct sequencer subscription transport")
            health.associatedOnShutdownRunner.close()
          }
        (killSwitch, doneF)
      }
      .injectKillSwitch { case (killSwitch, _) => killSwitch }

    SequencerSubscriptionPekko(source, health)
  }

  override def subscribeUnauthenticated(request: SubscriptionRequest)(implicit
      traceContext: TraceContext
  ): SequencerSubscriptionPekko[SubscriptionError] =
    unsupportedUnauthenticatedSubscription

  override def subscriptionRetryPolicyPekko: SubscriptionErrorRetryPolicyPekko[SubscriptionError] =
    // unlikely there will be any errors with this direct transport implementation
    SubscriptionErrorRetryPolicyPekko.never

  override def handshake(request: HandshakeRequest)(implicit
      traceContext: TraceContext
  ): EitherT[Future, HandshakeRequestError, HandshakeResponse] =
    // never called - throwing an exception so tests fail if this ever changes
    throw new UnsupportedOperationException(
      "handshake is not implemented for DirectSequencerClientTransport"
    )

  override def downloadTopologyStateForInit(request: TopologyStateForInitRequest)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, TopologyStateForInitResponse] =
    throw new UnsupportedOperationException(
      "downloadTopologyStateForInit is not implemented for DirectSequencerClientTransport"
    )
}

object DirectSequencerClientTransport {
  sealed trait SubscriptionError extends Product with Serializable with PrettyPrinting {
    override def pretty: Pretty[SubscriptionError.this.type] = adHocPrettyInstance
  }
  final case class SubscriptionCreationError(error: CreateSubscriptionError)
      extends SubscriptionError
  final case class SequencedEventError(error: SequencerSubscriptionError.SequencedEventError)
      extends SubscriptionError

  private class DirectSequencerClientTransportHealth(override protected val logger: TracedLogger)
      extends AtomicHealthComponent {
    override def name: String = "direct-sequencer-client-transport"

    override protected def initialHealthState: ComponentHealthState =
      ComponentHealthState.Ok()

    override lazy val associatedOnShutdownRunner: AutoCloseable & OnShutdownRunner =
      new OnShutdownRunner.PureOnShutdownRunner(logger)
  }
}
