// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports.replay

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.metrics.api.MetricsContext.withEmptyMetricsContext
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.HashPurpose
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.{MetricValue, SequencerClientMetrics}
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.sequencing.client.SendAsyncClientError.SendAsyncClientResponseError
import com.digitalasset.canton.sequencing.client.transports.{
  SequencerClientTransport,
  SequencerClientTransportCommon,
  SequencerClientTransportPekko,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.{
  OrdinarySerializedEvent,
  SequencerClientRecorder,
  SerializedEventHandler,
}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.topology.store.StoredTopologyTransactions
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.tracing.{NoTracing, TraceContext, Traced}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{ErrorUtil, OptionUtil, PekkoUtil}
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.Status
import io.opentelemetry.sdk.metrics.data.MetricData
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}

import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.DurationConverters.*
import scala.util.chaining.*

/** Replays previously recorded sends against the configured sequencer and using a real sequencer client transport.
  * Records the latencies/rates to complete the send itself, and latencies/rates for an event that was caused by the send to be witnessed.
  * These metrics are currently printed to stdout.
  * Sequencers are able to drop sends so to know when all sends have likely been sequenced we simply wait for a period
  * where no events are received for a configurable duration. This isn't perfect as technically a sequencer could stall,
  * however the inflight gauge will report a number greater than 0 indicating that these sends have gone missing.
  * Clients are responsible for interacting with the transport to initiate a replay and wait for observed events to
  * be idle. A reference can be obtained to this transport component by waiting on the future provided in [[ReplayAction.SequencerSends]].
  * This testing transport is very stateful and the metrics will only make sense for a single replay,
  * however currently multiple or even concurrent calls are not prevented (just don't).
  */
trait ReplayingSendsSequencerClientTransport extends SequencerClientTransportCommon {
  import ReplayingSendsSequencerClientTransport.*
  def replay(sendParallelism: Int): Future[SendReplayReport]

  def waitForIdle(
      duration: FiniteDuration,
      startFromCounter: SequencerCounter = SequencerCounter.Genesis,
  ): Future[EventsReceivedReport]

  /** Dump the submission related metrics into a string for periodic reporting during the replay test */
  def metricReport(snapshot: Seq[MetricData]): String
}

object ReplayingSendsSequencerClientTransport {
  final case class SendReplayReport(
      successful: Int = 0,
      overloaded: Int = 0,
      errors: Int = 0,
      shutdowns: Int = 0,
  )(
      sendDuration: => Option[java.time.Duration]
  ) {
    def update(
        result: UnlessShutdown[Either[SendAsyncClientError, Unit]]
    ): SendReplayReport =
      result match {
        case Outcome(Left(SendAsyncClientError.RequestRefused(_: SendAsyncError.Overloaded))) =>
          copy(overloaded = overloaded + 1)
        case Outcome(Left(_)) => copy(errors = errors + 1)
        case Outcome(Right(_)) => copy(successful = successful + 1)
        case AbortedDueToShutdown => copy(shutdowns = shutdowns + 1)
      }

    def copy(
        successful: Int = this.successful,
        overloaded: Int = this.overloaded,
        errors: Int = this.errors,
        shutdowns: Int = this.shutdowns,
    ): SendReplayReport = SendReplayReport(successful, overloaded, errors, shutdowns)(sendDuration)

    lazy val total: Int = successful + overloaded + errors

    override def toString: String = {
      val durationSecsText = sendDuration.map(_.getSeconds).map(secs => s"${secs}s").getOrElse("?")
      s"Sent $total send requests in $durationSecsText ($successful successful, $overloaded overloaded, $errors errors)"
    }
  }

  final case class EventsReceivedReport(
      elapsedDuration: FiniteDuration,
      totalEventsReceived: Int,
      finishedAtCounter: SequencerCounter,
  ) {
    override def toString: String =
      s"Received $totalEventsReceived events within ${elapsedDuration.toSeconds}s"
  }

}

abstract class ReplayingSendsSequencerClientTransportCommon(
    protocolVersion: ProtocolVersion,
    recordedPath: Path,
    replaySendsConfig: ReplayAction.SequencerSends,
    member: Member,
    underlyingTransport: SequencerClientTransportCommon,
    requestSigner: RequestSigner,
    metrics: SequencerClientMetrics,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext, materializer: Materializer)
    extends ReplayingSendsSequencerClientTransport
    with NamedLogging
    with NoTracing
    with FlagCloseableAsync {
  import ReplayingSendsSequencerClientTransport.*

  private val pendingSends = TrieMap[MessageId, CantonTimestamp]()
  private val firstSend = new AtomicReference[Option[CantonTimestamp]](None)
  private val lastSend = new AtomicReference[Option[CantonTimestamp]](None)

  private val submissionRequests: List[SubmissionRequest] = withNewTraceContext {
    implicit traceContext =>
      logger.debug("Loading recorded submission requests")
      ErrorUtil.withThrowableLogging {
        SequencerClientRecorder.loadSubmissions(recordedPath, logger)
      }
  }

  // Signals to the tests that this transport is ready to interact with
  replaySendsConfig.publishTransport(this)

  private def sendDuration: Option[java.time.Duration] =
    OptionUtil
      .zipWith(firstSend.get().map(_.toInstant), lastSend.get().map(_.toInstant))(
        java.time.Duration.between
      )

  private def replaySubmit(
      submission: SubmissionRequest
  ): FutureUnlessShutdown[Either[SendAsyncClientError, Unit]] = {
    val startedAt = CantonTimestamp.now()
    // we'll correlate received events by looking at their message-id and calculate the
    // latency of the send by comparing now to the time the event eventually arrives
    pendingSends.put(submission.messageId, startedAt).discard

    // Picking a correct max sequencing time could be technically difficult,
    // so instead we pick the biggest point in time that should ensure the sequencer always
    // attempts to sequence valid sends
    def extendMaxSequencingTime(submission: SubmissionRequest): SubmissionRequest =
      submission.updateMaxSequencingTime(maxSequencingTime = CantonTimestamp.MaxValue)

    def handleSendResult(
        result: Either[SendAsyncClientError, Unit]
    ): Either[SendAsyncClientError, Unit] =
      withEmptyMetricsContext { implicit metricsContext =>
        result.tap {
          case Left(SendAsyncClientError.RequestRefused(_: SendAsyncError.Overloaded)) =>
            logger.warn(
              s"Sequencer is overloaded and rejected our send. Please tune the sequencer to handle more concurrent requests."
            )
            metrics.submissions.overloaded.inc()

          case Left(error) =>
            // log, increase error counter, then ignore
            logger.warn(s"Send request failed: $error")

          case Right(_) =>
            // we've successfully sent the send request
            metrics.submissions.inFlight.inc()
            val sentAt = CantonTimestamp.now()
            metrics.submissions.sends
              .update(java.time.Duration.between(startedAt.toInstant, sentAt.toInstant))
        }
      }

    def updateTimestamps[A](item: A): A = {
      val now = CantonTimestamp.now()
      // only set the first send timestamp if none have been resent
      firstSend.compareAndSet(None, Some(now))
      lastSend.set(Some(now))
      item
    }

    TraceContext.withNewTraceContext { implicit traceContext =>
      val withExtendedMst = extendMaxSequencingTime(submission)
      val sendET = for {
        // We need a new signature because we've modified the max sequencing time.
        signedRequest <- requestSigner
          .signRequest(
            withExtendedMst,
            HashPurpose.SubmissionRequestSignature,
          )
          .leftMap(error =>
            SendAsyncClientError.RequestRefused(SendAsyncError.RequestRefused(error))
          )
        _ <- underlyingTransport
          .sendAsyncSigned(
            signedRequest,
            replaySendsConfig.sendTimeout.toScala,
          )
      } yield ()

      sendET.value
        .map(handleSendResult)
        .map(updateTimestamps)
    }
  }

  override def replay(sendParallelism: Int): Future[SendReplayReport] =
    withNewTraceContext { implicit traceContext =>
      logger.info(s"Replaying ${submissionRequests.size} sends")

      val submissionReplay = Source(submissionRequests)
        .mapAsyncUnordered(sendParallelism)(replaySubmit(_).unwrap)
        .toMat(Sink.fold(SendReplayReport()(sendDuration))(_.update(_)))(Keep.right)

      PekkoUtil.runSupervised(
        submissionReplay,
        errorLogMessagePrefix = "Failed to run submission replay",
      )
    }

  override def waitForIdle(
      duration: FiniteDuration,
      startFromCounter: SequencerCounter = SequencerCounter.Genesis,
  ): Future[EventsReceivedReport] = {
    val monitor = new SimpleIdlenessMonitor(startFromCounter, duration, timeouts, loggerFactory)

    monitor.idleF transform { result =>
      monitor.close()

      result
    }
  }

  /** Dump the submission related metrics into a string for periodic reporting during the replay test */
  override def metricReport(snapshot: Seq[MetricData]): String = {
    val metricName = metrics.submissions.prefix.toString()
    val out = snapshot.flatMap {
      case metric if metric.getName.startsWith(metricName) => MetricValue.fromMetricData(metric)
      case _ => Seq.empty
    }
    out.show
  }

  protected def subscribe(
      request: SubscriptionRequest,
      handler: SerializedEventHandler[NotUsed],
  ): AutoCloseable

  /** Monitor that when created subscribes the underlying transports and waits for Deliver or DeliverError events
    * to stop being observed for the given [[idlenessDuration]] (suggesting that there are no more events being
    * produced for the member).
    */
  private class SimpleIdlenessMonitor(
      readFrom: SequencerCounter,
      idlenessDuration: FiniteDuration,
      override protected val timeouts: ProcessingTimeout,
      protected val loggerFactory: NamedLoggerFactory,
  ) extends FlagCloseableAsync
      with NamedLogging {
    private case class State(
        startedAt: CantonTimestamp,
        lastEventAt: Option[CantonTimestamp],
        eventCounter: Int,
        lastCounter: SequencerCounter,
    )

    private val stateRef: AtomicReference[State] = new AtomicReference(
      State(
        startedAt = CantonTimestamp.now(),
        lastEventAt = None,
        eventCounter = 0,
        lastCounter = readFrom,
      )
    )
    private val idleP = Promise[EventsReceivedReport]()

    private def scheduleCheck(): Unit =
      performUnlessClosing(functionFullName) {
        val nextCheckDuration =
          idlenessDuration.toJava.minus(durationFromLastEventToNow(stateRef.get()))
        val _ = materializer.scheduleOnce(nextCheckDuration.toScala, () => checkIfIdle())
      }.onShutdown(())

    scheduleCheck() // kick off checks

    private def updateLastDeliver(counter: SequencerCounter): Unit = {
      val _ = stateRef.updateAndGet { case state @ State(_, _, eventCounter, _) =>
        state.copy(
          lastEventAt = Some(CantonTimestamp.now()),
          lastCounter = counter,
          eventCounter = eventCounter + 1,
        )
      }
    }

    private def checkIfIdle(): Unit = {
      val stateSnapshot = stateRef.get()
      val lastEventTime = stateSnapshot.lastEventAt.getOrElse(stateSnapshot.startedAt).toInstant
      val elapsedDuration =
        java.time.Duration.between(stateSnapshot.startedAt.toInstant, lastEventTime)
      val isIdle = durationFromLastEventToNow(stateSnapshot).compareTo(idlenessDuration.toJava) >= 0

      if (isIdle) {
        if (pendingSends.sizeIs > 0) {
          idleP
            .tryFailure(
              new IllegalStateException(s"There are ${pendingSends.size} pending send requests")
            )
            .discard
        } else {
          idleP
            .trySuccess(
              EventsReceivedReport(
                elapsedDuration.toScala,
                totalEventsReceived = stateSnapshot.eventCounter,
                finishedAtCounter = stateSnapshot.lastCounter,
              )
            )
            .discard
        }
      } else {
        scheduleCheck() // schedule the next check
      }
    }

    private def durationFromLastEventToNow(stateSnapshot: State) = {
      val from = stateSnapshot.lastEventAt.getOrElse(stateSnapshot.startedAt)
      java.time.Duration.between(from.toInstant, Instant.now())
    }

    private def updateMetrics(event: SequencedEvent[ClosedEnvelope]): Unit =
      withEmptyMetricsContext { implicit metricsContext =>
        val messageIdO: Option[MessageId] = event match {
          case Deliver(_, _, _, messageId, _, _, _) => messageId
          case DeliverError(_, _, _, messageId, _, _) => Some(messageId)
          case _ => None
        }

        messageIdO.flatMap(pendingSends.remove) foreach { sentAt =>
          val latency = java.time.Duration.between(sentAt.toInstant, Instant.now())
          metrics.submissions.inFlight.dec()
          metrics.submissions.sequencingTime.update(latency)
        }
      }

    private def handle(
        event: OrdinarySerializedEvent
    ): FutureUnlessShutdown[Either[NotUsed, Unit]] = {
      val content = event.signedEvent.content

      updateMetrics(content)
      updateLastDeliver(content.counter)

      FutureUnlessShutdown.pure(Either.unit)
    }

    val idleF: Future[EventsReceivedReport] = idleP.future

    private val subscription =
      subscribe(SubscriptionRequest(member, readFrom, protocolVersion), handle)

    override protected def closeAsync(): Seq[AsyncOrSyncCloseable] =
      Seq(
        SyncCloseable("idleness-subscription", subscription.close())
      )
  }

  /** We're replaying sends so shouldn't allow the app to send any new ones */
  override def sendAsyncSigned(
      request: SignedContent[SubmissionRequest],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SendAsyncClientResponseError, Unit] =
    EitherT.rightT(())

  override def acknowledgeSigned(request: SignedContent[AcknowledgeRequest])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Boolean] =
    EitherT.rightT(true)

  override def getTrafficStateForMember(request: GetTrafficStateForMemberRequest)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, GetTrafficStateForMemberResponse] =
    EitherT.pure(GetTrafficStateForMemberResponse(None, protocolVersion))

  override def downloadTopologyStateForInit(request: TopologyStateForInitRequest)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, TopologyStateForInitResponse] =
    EitherT.rightT(TopologyStateForInitResponse(Traced(StoredTopologyTransactions.empty)))

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Seq(
    SyncCloseable("underlying-transport", underlyingTransport.close())
  )

}

class ReplayingSendsSequencerClientTransportImpl(
    protocolVersion: ProtocolVersion,
    recordedPath: Path,
    replaySendsConfig: ReplayAction.SequencerSends,
    member: Member,
    val underlyingTransport: SequencerClientTransport & SequencerClientTransportPekko,
    requestSigner: RequestSigner,
    metrics: SequencerClientMetrics,
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext, materializer: Materializer)
    extends ReplayingSendsSequencerClientTransportCommon(
      protocolVersion,
      recordedPath,
      replaySendsConfig,
      member,
      underlyingTransport,
      requestSigner,
      metrics,
      timeouts,
      loggerFactory,
    )
    with SequencerClientTransport
    with SequencerClientTransportPekko {

  override def logout()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Status, Unit] =
    EitherT.pure(())

  override def subscribe[E](request: SubscriptionRequest, handler: SerializedEventHandler[E])(
      implicit traceContext: TraceContext
  ): SequencerSubscription[E] = new SequencerSubscription[E] {
    override protected def loggerFactory: NamedLoggerFactory =
      ReplayingSendsSequencerClientTransportImpl.this.loggerFactory

    override protected def timeouts: ProcessingTimeout =
      ReplayingSendsSequencerClientTransportImpl.this.timeouts

    override private[canton] def complete(reason: SubscriptionCloseReason[E])(implicit
        traceContext: TraceContext
    ): Unit = closeReasonPromise.trySuccess(reason).discard[Boolean]
  }

  override def subscriptionRetryPolicy: SubscriptionErrorRetryPolicy =
    SubscriptionErrorRetryPolicy.never

  override protected def subscribe(
      request: SubscriptionRequest,
      handler: SerializedEventHandler[NotUsed],
  ): AutoCloseable =
    underlyingTransport.subscribe(request, handler)

  override type SubscriptionError = underlyingTransport.SubscriptionError

  override def subscribe(request: SubscriptionRequest)(implicit
      traceContext: TraceContext
  ): SequencerSubscriptionPekko[SubscriptionError] = underlyingTransport.subscribe(request)

  override def subscriptionRetryPolicyPekko: SubscriptionErrorRetryPolicyPekko[SubscriptionError] =
    SubscriptionErrorRetryPolicyPekko.never
}

class ReplayingSendsSequencerClientTransportPekko(
    protocolVersion: ProtocolVersion,
    recordedPath: Path,
    replaySendsConfig: ReplayAction.SequencerSends,
    member: Member,
    underlyingTransport: SequencerClientTransportPekko & SequencerClientTransport,
    requestSigner: RequestSigner,
    metrics: SequencerClientMetrics,
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext, materializer: Materializer)
// TODO(#13789) Extend `ReplayingSendsSequencerClientTransportCommon`
    extends ReplayingSendsSequencerClientTransportImpl(
      protocolVersion,
      recordedPath,
      replaySendsConfig,
      member,
      underlyingTransport,
      requestSigner,
      metrics,
      timeouts,
      loggerFactory,
    )
    with SequencerClientTransportPekko {

  override protected def subscribe(
      request: SubscriptionRequest,
      handler: SerializedEventHandler[NotUsed],
  ): AutoCloseable = {
    val ((killSwitch, _), doneF) = subscribe(request).source
      .mapAsync(parallelism = 10)(eventKS =>
        eventKS.value
          .traverse { event =>
            handler(event)
          }
          .tapOnShutdown(eventKS.killSwitch.shutdown())
          .onShutdown(Left(SubscriptionCloseReason.Shutdown))
      )
      .watchTermination()(Keep.both)
      .to(Sink.ignore)
      .run()
    new AutoCloseable {
      override def close(): Unit = {
        killSwitch.shutdown()
        timeouts.closing.await_("closing subscription")(doneF)
      }
    }
  }
}
