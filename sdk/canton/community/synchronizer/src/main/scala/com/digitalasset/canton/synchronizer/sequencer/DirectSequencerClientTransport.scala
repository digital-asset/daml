// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.health.{AtomicHealthComponent, ComponentHealthState}
import com.digitalasset.canton.lifecycle.{
  FutureUnlessShutdown,
  OnShutdownRunner,
  SyncCloseable,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.sequencing.SerializedEventHandler
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.sequencing.client.SendAsyncClientError.SendAsyncClientResponseError
import com.digitalasset.canton.sequencing.client.transports.{
  SequencerClientTransport,
  SequencerClientTransportPekko,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.synchronizer.sequencer.errors.CreateSubscriptionError
import com.digitalasset.canton.synchronizer.sequencer.errors.CreateSubscriptionError.ShutdownError
import com.digitalasset.canton.synchronizer.sequencing.service.DirectSequencerSubscriptionFactory
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil.DelayedKillSwitch
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{ErrorUtil, FutureUnlessShutdownUtil, PekkoUtil}
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.Status
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
    protocolVersion: ProtocolVersion,
)(implicit materializer: Materializer, ec: ExecutionContext)
    extends SequencerClientTransport
    with SequencerClientTransportPekko
    with NamedLogging {
  import DirectSequencerClientTransport.*

  override def logout()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Status, Unit] =
    // In-process connection is not authenticated
    EitherT.pure(())

  private val subscriptionFactory =
    new DirectSequencerSubscriptionFactory(sequencer, timeouts, loggerFactory)

  override def sendAsyncSigned(
      request: SignedContent[SubmissionRequest],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SendAsyncClientResponseError, Unit] =
    sequencer
      .sendAsyncSigned(request)
      .leftMap(SendAsyncClientError.RequestRefused.apply)

  override def acknowledgeSigned(request: SignedContent[AcknowledgeRequest])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Boolean] =
    sequencer
      .acknowledgeSigned(request)
      .map(_ => true)

  override def getTrafficStateForMember(request: GetTrafficStateForMemberRequest)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, GetTrafficStateForMemberResponse] =
    sequencer
      .getTrafficStateAt(request.member, request.timestamp)
      .map { trafficStateO =>
        GetTrafficStateForMemberResponse(trafficStateO, protocolVersion)
      }
      .leftMap(_.toString)

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
                  s"Direct transport subscriptions must not trigger subscription errors such as $error"
                )
            },
          )
          .thereafter {
            case Success(UnlessShutdown.Outcome(Right(subscription))) =>
              closeReasonPromise.completeWith(subscription.closeReason)

              performUnlessClosing(functionFullName) {
                subscriptionRef.set(Some(subscription))
              } onShutdown {
                subscription.close()
              }
            case Success(UnlessShutdown.Outcome(Left(value))) =>
              closeReasonPromise.trySuccess(Fatal(value.toString)).discard[Boolean]
            case Failure(exception) =>
              closeReasonPromise.tryFailure(exception).discard[Boolean]
            case Success(UnlessShutdown.AbortedDueToShutdown) =>
              closeReasonPromise.trySuccess(SubscriptionCloseReason.Shutdown).discard[Boolean]
          }
      FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
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
      .unwrap
      .map {
        case UnlessShutdown.AbortedDueToShutdown =>
          Source
            .single(Left(SubscriptionCreationError(ShutdownError)))
            .mapMaterializedValue((_: NotUsed) =>
              (PekkoUtil.noOpKillSwitch, FutureUnlessShutdown.pure(Done))
            )
        case UnlessShutdown.Outcome(Left(creationError)) =>
          Source
            .single(Left(SubscriptionCreationError(creationError)))
            .mapMaterializedValue((_: NotUsed) =>
              (PekkoUtil.noOpKillSwitch, FutureUnlessShutdown.pure(Done))
            )
        case UnlessShutdown.Outcome(Right(source)) =>
          source.map(_.leftMap(SequencedEventError.apply))
      }
    val health = new DirectSequencerClientTransportHealth(logger)
    val source = Source
      .futureSource(sourceF)
      .watchTermination() { (matF, terminationF) =>
        val directExecutionContext = DirectExecutionContext(noTracingLogger)
        val killSwitchF = matF.map { case (killSwitch, _) => killSwitch }(directExecutionContext)
        val killSwitch = new DelayedKillSwitch(killSwitchF, noTracingLogger)
        val doneF = matF
          .flatMap { case (_, doneF) => doneF.unwrap }(directExecutionContext)
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

  override def subscriptionRetryPolicyPekko: SubscriptionErrorRetryPolicyPekko[SubscriptionError] =
    // unlikely there will be any errors with this direct transport implementation
    SubscriptionErrorRetryPolicyPekko.never

  override def downloadTopologyStateForInit(request: TopologyStateForInitRequest)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, TopologyStateForInitResponse] =
    throw new UnsupportedOperationException(
      "downloadTopologyStateForInit is not implemented for DirectSequencerClientTransport"
    )
}

object DirectSequencerClientTransport {
  sealed trait SubscriptionError extends Product with Serializable with PrettyPrinting {
    override protected def pretty: Pretty[SubscriptionError.this.type] = adHocPrettyInstance
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
