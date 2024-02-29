// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import cats.implicits.catsSyntaxOptionId
import cats.syntax.alternative.*
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.daml.metrics.Timed
import com.daml.metrics.api.MetricsContext
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.*
import com.digitalasset.canton.crypto.{CryptoPureApi, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.{
  CloseableHealthComponent,
  ComponentHealthState,
  DelegatingMutableHealthComponent,
  HealthComponent,
}
import com.digitalasset.canton.lifecycle.Lifecycle.toCloseableOption
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.pretty.{CantonPrettyPrinter, Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.SequencerClientMetrics
import com.digitalasset.canton.protocol.DomainParameters.MaxRequestSize
import com.digitalasset.canton.protocol.DomainParametersLookup.SequencerDomainParameters
import com.digitalasset.canton.protocol.DynamicDomainParametersLookup
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.resource.DbStorage.PassiveInstanceException
import com.digitalasset.canton.sequencing.SequencerAggregator.MessageAggregationConfig
import com.digitalasset.canton.sequencing.SequencerAggregatorPekko.{
  HasSequencerSubscriptionFactoryPekko,
  SubscriptionControl,
}
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.client.PeriodicAcknowledgements.FetchCleanTimestamp
import com.digitalasset.canton.sequencing.client.SendCallback.CallbackFuture
import com.digitalasset.canton.sequencing.client.SequencerClient.SequencerTransports
import com.digitalasset.canton.sequencing.client.SequencerClientSubscriptionError.*
import com.digitalasset.canton.sequencing.client.transports.{
  SequencerClientTransport,
  SequencerClientTransportPekko,
}
import com.digitalasset.canton.sequencing.handlers.{
  CleanSequencerCounterTracker,
  StoreSequencedEvent,
  ThrottlingApplicationEventHandler,
}
import com.digitalasset.canton.sequencing.protocol.{AggregationRule, *}
import com.digitalasset.canton.store.CursorPrehead.SequencerCounterCursorPrehead
import com.digitalasset.canton.store.SequencedEventStore.PossiblyIgnoredSequencedEvent
import com.digitalasset.canton.store.*
import com.digitalasset.canton.time.{Clock, DomainTimeTracker}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.{HasTraceContext, Spanning, TraceContext, Traced}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.FutureUtil.defaultStackTraceFilter
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.util.PekkoUtil.{CombinedKillSwitch, WithKillSwitch}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.retry.RetryUtil.AllExnRetryable
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DiscardOps, SequencerAlias, SequencerCounter}
import com.google.common.annotations.VisibleForTesting
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.apache.pekko.stream.{KillSwitch, KillSwitches, Materializer}
import org.apache.pekko.{Done, NotUsed}
import org.slf4j.event.Level

import java.nio.file.Path
import java.time.Duration as JDuration
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}
import scala.concurrent.*
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}

trait SequencerClient extends SequencerClientSend with FlagCloseable {

  /** Sends a request to sequence a deliver event to the sequencer.
    * This method merely dispatches to one of the other methods (`sendAsync` or `sendAsyncUnauthenticated`)
    * depending if member is Authenticated or Unauthenticated.
    */
  def sendAsyncUnauthenticatedOrNot(
      batch: Batch[DefaultOpenEnvelope],
      sendType: SendType = SendType.Other,
      topologyTimestamp: Option[CantonTimestamp] = None,
      maxSequencingTime: CantonTimestamp = generateMaxSequencingTime,
      messageId: MessageId = generateMessageId,
      aggregationRule: Option[AggregationRule] = None,
      callback: SendCallback = SendCallback.empty,
      amplify: Boolean = false,
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncClientError, Unit]

  /** Does the same as [[sendAsync]], except that this method is supposed to be used
    * only by unauthenticated members for very specific operations that do not require authentication
    * such as requesting that a participant's topology data gets accepted by the topology manager
    */
  def sendAsyncUnauthenticated(
      batch: Batch[DefaultOpenEnvelope],
      sendType: SendType = SendType.Other,
      maxSequencingTime: CantonTimestamp = generateMaxSequencingTime,
      messageId: MessageId = generateMessageId,
      callback: SendCallback = SendCallback.empty,
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncClientError, Unit]

  /** Create a subscription for sequenced events for this member,
    * starting after the prehead in the `sequencerCounterTrackerStore`.
    *
    * The `eventHandler` is monitored by [[com.digitalasset.canton.sequencing.handlers.CleanSequencerCounterTracker]]
    * so that the `sequencerCounterTrackerStore` advances the prehead
    * when (a batch of) events has been successfully processed by the `eventHandler` (synchronously and asynchronously).
    *
    * @see subscribe for the description of the `eventHandler` and the `timeTracker`
    */
  def subscribeTracking(
      sequencerCounterTrackerStore: SequencerCounterTrackerStore,
      eventHandler: PossiblyIgnoredApplicationHandler[ClosedEnvelope],
      timeTracker: DomainTimeTracker,
      onCleanHandler: Traced[SequencerCounterCursorPrehead] => Unit = _ => (),
  )(implicit traceContext: TraceContext): Future[Unit]

  /** Create a subscription for sequenced events for this member,
    * starting after the last event in the [[com.digitalasset.canton.store.SequencedEventStore]] up to `priorTimestamp`.
    * A sequencer client can only have a single subscription - additional subscription attempts will throw an exception.
    * When an event is received, we will check the pending sends and invoke the provided call-backs with the send result
    * (which can be deliver or timeout) before invoking the `eventHandler`.
    *
    * If the [[com.digitalasset.canton.store.SequencedEventStore]] contains events after `priorTimestamp`,
    * the handler is first fed with these events before the subscription is established,
    * starting at the last event found in the [[com.digitalasset.canton.store.SequencedEventStore]].
    *
    * @param priorTimestamp      The timestamp of the event prior to where the event processing starts.
    *                            If [[scala.None$]], the subscription starts at the [[initialCounterLowerBound]].
    * @param cleanPreheadTsO     The timestamp of the clean prehead sequencer counter, if known.
    * @param eventHandler        A function handling the events.
    * @param timeTracker         Tracker for operations requiring the current domain time. Only updated with received events and not previously stored events.
    * @param fetchCleanTimestamp A function for retrieving the latest clean timestamp to use for periodic acknowledgements
    * @return The future completes after the subscription has been established or when an error occurs before that.
    *         In particular, synchronous processing of events from the [[com.digitalasset.canton.store.SequencedEventStore]]
    *         runs before the future completes.
    */
  def subscribeAfter(
      priorTimestamp: CantonTimestamp,
      cleanPreheadTsO: Option[CantonTimestamp],
      eventHandler: PossiblyIgnoredApplicationHandler[ClosedEnvelope],
      timeTracker: DomainTimeTracker,
      fetchCleanTimestamp: PeriodicAcknowledgements.FetchCleanTimestamp,
  )(implicit traceContext: TraceContext): Future[Unit]

  /** Does the same as [[subscribeAfter]], except that this method is supposed to be used
    * only by unauthenticated members
    */
  def subscribeAfterUnauthenticated(
      priorTimestamp: CantonTimestamp,
      eventHandler: PossiblyIgnoredApplicationHandler[ClosedEnvelope],
      timeTracker: DomainTimeTracker,
  )(implicit traceContext: TraceContext): Future[Unit]

  /** Acknowledge that we have successfully processed all events up to and including the given timestamp.
    * The client should then never subscribe for events from before this point.
    */
  private[client] def acknowledge(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  def acknowledgeSigned(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit]

  /** The sequencer counter at which the first subscription starts */
  protected def initialCounterLowerBound: SequencerCounter
}

trait RichSequencerClient extends SequencerClient {

  def healthComponent: CloseableHealthComponent

  def changeTransport(
      sequencerTransports: SequencerTransports[?]
  )(implicit traceContext: TraceContext): Future[Unit]

  /** Future which is completed when the client is not functional any more and is ready to be closed.
    * The value with which the future is completed will indicate the reason for completion.
    */
  def completion: Future[SequencerClient.CloseReason]

  /** Returns a future that completes after asynchronous processing has completed for all events
    * whose synchronous processing has been completed prior to this call. May complete earlier if event processing
    * has failed.
    */
  @VisibleForTesting
  def flush(): Future[Unit]
}

abstract class SequencerClientImpl(
    val domainId: DomainId,
    val member: Member,
    sequencerTransports: SequencerTransports[?],
    val config: SequencerClientConfig,
    testingConfig: TestingConfigInternal,
    val protocolVersion: ProtocolVersion,
    domainParametersLookup: DynamicDomainParametersLookup[SequencerDomainParameters],
    override val timeouts: ProcessingTimeout,
    eventValidatorFactory: SequencedEventValidatorFactory,
    clock: Clock,
    val requestSigner: RequestSigner,
    protected val sequencedEventStore: SequencedEventStore,
    sendTracker: SendTracker,
    metrics: SequencerClientMetrics,
    recorderO: Option[SequencerClientRecorder],
    replayEnabled: Boolean,
    cryptoPureApi: CryptoPureApi,
    loggingConfig: LoggingConfig,
    val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
    override protected val initialCounterLowerBound: SequencerCounter,
)(implicit executionContext: ExecutionContext, tracer: Tracer)
    extends SequencerClient
    with FlagCloseableAsync
    with NamedLogging
    with Spanning
    with HasCloseContext {

  protected val sequencersTransportState =
    new SequencersTransportState(
      sequencerTransports,
      timeouts,
      loggerFactory,
    )

  private lazy val printer =
    new CantonPrettyPrinter(loggingConfig.api.maxStringLength, loggingConfig.api.maxMessageLines)

  override def sendAsyncUnauthenticatedOrNot(
      batch: Batch[DefaultOpenEnvelope],
      sendType: SendType,
      topologyTimestamp: Option[CantonTimestamp],
      maxSequencingTime: CantonTimestamp,
      messageId: MessageId,
      aggregationRule: Option[AggregationRule],
      callback: SendCallback,
      amplify: Boolean,
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncClientError, Unit] = {
    member match {
      case _: AuthenticatedMember =>
        sendAsync(
          batch = batch,
          sendType = sendType,
          topologyTimestamp = topologyTimestamp,
          maxSequencingTime = maxSequencingTime,
          messageId = messageId,
          aggregationRule = aggregationRule,
          callback = callback,
        )
      case _: UnauthenticatedMemberId =>
        sendAsyncUnauthenticated(
          batch = batch,
          sendType = sendType,
          maxSequencingTime = maxSequencingTime,
          messageId = messageId,
          callback = callback,
        )
    }
  }

  override def sendAsync(
      batch: Batch[DefaultOpenEnvelope],
      sendType: SendType,
      topologyTimestamp: Option[CantonTimestamp],
      maxSequencingTime: CantonTimestamp,
      messageId: MessageId,
      aggregationRule: Option[AggregationRule],
      callback: SendCallback,
      amplify: Boolean,
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncClientError, Unit] =
    for {
      _ <- EitherT.cond[Future](
        member.isAuthenticated,
        (),
        SendAsyncClientError.RequestInvalid(
          "Only authenticated members can use the authenticated send operation"
        ): SendAsyncClientError,
      )
      // TODO(#12950): Validate that group addresses map to at least one member
      _ <- EitherT.cond[Future](
        topologyTimestamp.isEmpty || batch.envelopes.forall(
          _.recipients.allRecipients.forall {
            case MemberRecipient(m) => m.isAuthenticated
            case _ => true
          }
        ),
        (),
        SendAsyncClientError.RequestInvalid(
          "Requests addressed to unauthenticated members must not specify a topology timestamp"
        ): SendAsyncClientError,
      )
      result <- sendAsyncInternal(
        batch,
        requiresAuthentication = true,
        sendType,
        topologyTimestamp,
        maxSequencingTime,
        messageId,
        aggregationRule,
        callback,
        amplify,
      )
    } yield result

  /** Does the same as [[sendAsync]], except that this method is supposed to be used
    * only by unauthenticated members for very specific operations that do not require authentication
    * such as requesting that a participant's topology data gets accepted by the topology manager
    */
  override def sendAsyncUnauthenticated(
      batch: Batch[DefaultOpenEnvelope],
      sendType: SendType = SendType.Other,
      maxSequencingTime: CantonTimestamp = generateMaxSequencingTime,
      messageId: MessageId = generateMessageId,
      callback: SendCallback = SendCallback.empty,
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncClientError, Unit] =
    if (member.isAuthenticated)
      EitherT.leftT(
        SendAsyncClientError.RequestInvalid(
          "Only unauthenticated members can use the unauthenticated send operation"
        )
      )
    else
      sendAsyncInternal(
        batch,
        requiresAuthentication = false,
        sendType,
        // Requests involving unauthenticated members must not specify a topology timestamp
        topologyTimestamp = None,
        maxSequencingTime = maxSequencingTime,
        messageId = messageId,
        aggregationRule = None,
        callback = callback,
        amplify = false,
      )

  private def checkRequestSize(
      request: SubmissionRequest,
      maxRequestSize: MaxRequestSize,
  ): Either[SendAsyncClientError, Unit] = {
    // We're ignoring the size of the SignedContent wrapper here.
    // TODO(#12320) Look into what we really want to do here
    val serializedRequestSize = request.toProtoV30.serializedSize

    Either.cond(
      serializedRequestSize <= maxRequestSize.unwrap,
      (),
      SendAsyncClientError.RequestInvalid(
        s"Batch size ($serializedRequestSize bytes) is exceeding maximum size ($maxRequestSize bytes) for domain $domainId"
      ),
    )
  }

  private def sendAsyncInternal(
      batch: Batch[DefaultOpenEnvelope],
      requiresAuthentication: Boolean,
      sendType: SendType,
      topologyTimestamp: Option[CantonTimestamp],
      maxSequencingTime: CantonTimestamp,
      messageId: MessageId,
      aggregationRule: Option[AggregationRule],
      callback: SendCallback,
      amplify: Boolean,
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncClientError, Unit] =
    withSpan("SequencerClient.sendAsync") { implicit traceContext => span =>
      val requestE = SubmissionRequest
        .create(
          member,
          messageId,
          sendType.isRequest,
          Batch.closeEnvelopes(batch),
          maxSequencingTime,
          topologyTimestamp,
          aggregationRule,
          SubmissionRequest.protocolVersionRepresentativeFor(protocolVersion),
        )
        .leftMap(err =>
          SendAsyncClientError.RequestInvalid(s"Unable to get submission request: $err")
        )

      if (loggingConfig.eventDetails) {
        requestE match {
          case Left(err) =>
            logger.debug(
              s"Will not send async batch ${printer.printAdHoc(batch)} because of invalid request: $err"
            )
          case Right(request) =>
            logger.debug(
              s"About to send async batch ${printer.printAdHoc(batch)} as request ${printer.printAdHoc(request)}"
            )
        }
      }

      span.setAttribute("member", member.show)
      span.setAttribute("message_id", messageId.unwrap)

      // avoid emitting a warning during the first sequencing of the topology snapshot
      val warnOnUsingDefaults = member match {
        case _: ParticipantId => true
        case _ => false
      }
      val domainParamsF = EitherTUtil.fromFuture(
        domainParametersLookup.getApproximateOrDefaultValue(warnOnUsingDefaults),
        throwable =>
          SendAsyncClientError.RequestFailed(
            s"failed to retrieve maxRequestSize because ${throwable.getMessage}"
          ),
      )

      def trackSend: EitherT[Future, SendAsyncClientError, Unit] =
        sendTracker
          .track(messageId, maxSequencingTime, callback)
          .leftMap[SendAsyncClientError] { case SavePendingSendError.MessageIdAlreadyTracked =>
            // we're already tracking this message id
            SendAsyncClientError.DuplicateMessageId
          }

      if (replayEnabled) {
        for {
          request <- EitherT.fromEither[Future](requestE)
          domainParams <- domainParamsF
          _ <- EitherT.fromEither[Future](checkRequestSize(request, domainParams.maxRequestSize))
        } yield {
          // Invoke the callback immediately, because it will not be triggered by replayed messages,
          // as they will very likely have mismatching message ids.
          val dummySendResult =
            SendResult.Success(
              Deliver.create(
                SequencerCounter.Genesis,
                CantonTimestamp.now(),
                domainId,
                messageIdO = None,
                Batch(List.empty, protocolVersion),
                topologyTimestampO = None,
                protocolVersion,
              )
            )
          callback(UnlessShutdown.Outcome(dummySendResult))
        }
      } else {
        for {
          request <- EitherT.fromEither[Future](requestE)
          domainParams <- domainParamsF
          _ <- EitherT.fromEither[Future](checkRequestSize(request, domainParams.maxRequestSize))
          _ <- trackSend
          _ = recorderO.foreach(_.recordSubmission(request))
          _ <- performSend(messageId, request, requiresAuthentication, amplify)
        } yield ()
      }
    }

  /** Perform the send, without any check.
    */
  private def performSend(
      messageId: MessageId,
      request: SubmissionRequest,
      requiresAuthentication: Boolean,
      amplify: Boolean,
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncClientError, Unit] = {
    EitherTUtil
      .timed(metrics.submissions.sends) {
        val timeout = timeouts.network.duration
        if (requiresAuthentication) {
          val (transports, amplifiableRequest) = if (amplify) {
            val transports = sequencersTransportState.amplifiedTransports
            val amplifiableRequest = if (transports.sizeIs > 1 && request.aggregationRule.isEmpty) {
              val aggregationRule =
                AggregationRule(NonEmpty(Seq, member), PositiveInt.one, protocolVersion)
              logger.debug(
                s"Adding aggregation rule $aggregationRule to submission request with message ID $messageId"
              )
              request.copy(aggregationRule = aggregationRule.some)
            } else request
            (transports, amplifiableRequest)
          } else { (NonEmpty(Seq, sequencersTransportState.transport), request) }

          for {
            signedContent <- requestSigner
              .signRequest(amplifiableRequest, HashPurpose.SubmissionRequestSignature)
              .leftMap { err =>
                val message = s"Error signing submission request $err"
                logger.error(message)
                SendAsyncClientError.RequestRefused(SendAsyncError.RequestRefused(message))
              }
            sendResults <- EitherT.right(
              transports.toNEF.parTraverse(_.sendAsyncSigned(signedContent, timeout).value)
            )
            (errors, successes) = sendResults.forgetNE.separate
            _ <- EitherT.cond[Future](
              successes.nonEmpty,
              (),
              errors match {
                case Seq(single) => single
                case multiple =>
                  SendAsyncClientError.RequestFailed(
                    s"Failed to send submission request to any sequencer: ${multiple.mkString(", ")}"
                  )
              },
            )
          } yield ()
        } else
          sequencersTransportState.transport.sendAsyncUnauthenticatedVersioned(request, timeout)
      }
      .leftSemiflatMap { err =>
        // increment appropriate error metrics
        err match {
          case SendAsyncClientError.RequestRefused(SendAsyncError.Overloaded(_)) =>
            metrics.submissions.overloaded.inc()
          case _ =>
        }

        // cancel pending send now as we know the request will never cause a sequenced result
        logger.debug(s"Cancelling the pending send as the sequencer returned error: $err")
        sendTracker.cancelPendingSend(messageId).map(_ => err)
      }
  }

  override def generateMaxSequencingTime: CantonTimestamp =
    clock.now.add(config.defaultMaxSequencingTimeOffset.asJava)

  override def generateMessageId: MessageId = MessageId.randomMessageId()

  /** Create a subscription for sequenced events for this member,
    * starting after the prehead in the `sequencerCounterTrackerStore`.
    *
    * The `eventHandler` is monitored by [[com.digitalasset.canton.sequencing.handlers.CleanSequencerCounterTracker]]
    * so that the `sequencerCounterTrackerStore` advances the prehead
    * when (a batch of) events has been successfully processed by the `eventHandler` (synchronously and asynchronously).
    *
    * @see subscribe for the description of the `eventHandler` and the `timeTracker`
    */
  def subscribeTracking(
      sequencerCounterTrackerStore: SequencerCounterTrackerStore,
      eventHandler: PossiblyIgnoredApplicationHandler[ClosedEnvelope],
      timeTracker: DomainTimeTracker,
      onCleanHandler: Traced[SequencerCounterCursorPrehead] => Unit = _ => (),
  )(implicit traceContext: TraceContext): Future[Unit] = {
    sequencerCounterTrackerStore.preheadSequencerCounter.flatMap { cleanPrehead =>
      val priorTimestamp = cleanPrehead.fold(CantonTimestamp.MinValue)(
        _.timestamp
      ) // Sequencer client will feed events right after this ts to the handler.
      val cleanSequencerCounterTracker = new CleanSequencerCounterTracker(
        sequencerCounterTrackerStore,
        onCleanHandler,
        loggerFactory,
      )
      subscribeAfter(
        priorTimestamp,
        cleanPrehead.map(_.timestamp),
        cleanSequencerCounterTracker(eventHandler),
        timeTracker,
        PeriodicAcknowledgements.fetchCleanCounterFromStore(sequencerCounterTrackerStore),
      )
    }
  }

  /** Create a subscription for sequenced events for this member,
    * starting after the last event in the [[com.digitalasset.canton.store.SequencedEventStore]] up to `priorTimestamp`.
    * A sequencer client can only have a single subscription - additional subscription attempts will throw an exception.
    * When an event is received, we will check the pending sends and invoke the provided call-backs with the send result
    * (which can be deliver or timeout) before invoking the `eventHandler`.
    *
    * If the [[com.digitalasset.canton.store.SequencedEventStore]] contains events after `priorTimestamp`,
    * the handler is first fed with these events before the subscription is established,
    * starting at the last event found in the [[com.digitalasset.canton.store.SequencedEventStore]].
    *
    * @param priorTimestamp The timestamp of the event prior to where the event processing starts.
    *                       If [[scala.None$]], the subscription starts at the [[com.digitalasset.canton.data.CounterCompanion.Genesis]].
    * @param cleanPreheadTsO The timestamp of the clean prehead sequencer counter, if known.
    * @param eventHandler A function handling the events.
    * @param timeTracker Tracker for operations requiring the current domain time. Only updated with received events and not previously stored events.
    * @param fetchCleanTimestamp A function for retrieving the latest clean timestamp to use for periodic acknowledgements
    * @return The future completes after the subscription has been established or when an error occurs before that.
    *         In particular, synchronous processing of events from the [[com.digitalasset.canton.store.SequencedEventStore]]
    *         runs before the future completes.
    */
  def subscribeAfter(
      priorTimestamp: CantonTimestamp,
      cleanPreheadTsO: Option[CantonTimestamp],
      eventHandler: PossiblyIgnoredApplicationHandler[ClosedEnvelope],
      timeTracker: DomainTimeTracker,
      fetchCleanTimestamp: PeriodicAcknowledgements.FetchCleanTimestamp,
  )(implicit traceContext: TraceContext): Future[Unit] =
    subscribeAfterInternal(
      priorTimestamp,
      cleanPreheadTsO,
      eventHandler,
      timeTracker,
      fetchCleanTimestamp,
      requiresAuthentication = true,
    )

  /** Does the same as [[subscribeAfter]], except that this method is supposed to be used
    * only by unauthenticated members
    *
    * The method does not verify the signature of the server.
    */
  def subscribeAfterUnauthenticated(
      priorTimestamp: CantonTimestamp,
      eventHandler: PossiblyIgnoredApplicationHandler[ClosedEnvelope],
      timeTracker: DomainTimeTracker,
  )(implicit traceContext: TraceContext): Future[Unit] =
    subscribeAfterInternal(
      priorTimestamp,
      // We do not track cleanliness for unauthenticated subscriptions
      cleanPreheadTsO = None,
      eventHandler,
      timeTracker,
      PeriodicAcknowledgements.noAcknowledgements,
      requiresAuthentication = false,
    )

  protected def subscribeAfterInternal(
      priorTimestamp: CantonTimestamp,
      cleanPreheadTsO: Option[CantonTimestamp],
      nonThrottledEventHandler: PossiblyIgnoredApplicationHandler[ClosedEnvelope],
      timeTracker: DomainTimeTracker,
      fetchCleanTimestamp: PeriodicAcknowledgements.FetchCleanTimestamp,
      requiresAuthentication: Boolean,
  )(implicit traceContext: TraceContext): Future[Unit]

  /** Acknowledge that we have successfully processed all events up to and including the given timestamp.
    * The client should then never subscribe for events from before this point.
    */
  private[client] def acknowledge(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    val request = AcknowledgeRequest(member, timestamp, protocolVersion)
    sequencersTransportState.transport.acknowledge(request)
  }

  def acknowledgeSigned(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit] = {
    val request = AcknowledgeRequest(member, timestamp, protocolVersion)
    for {
      signedRequest <- requestSigner.signRequest(request, HashPurpose.AcknowledgementSignature)
      _ <- sequencersTransportState.transport.acknowledgeSigned(signedRequest)
    } yield ()
  }

  protected val periodicAcknowledgementsRef =
    new AtomicReference[Option[PeriodicAcknowledgements]](None)
}

/** The sequencer client facilitates access to the individual domain sequencer. A client centralizes the
  * message signing operations, as well as the handling and storage of message receipts and delivery proofs,
  * such that this functionality does not have to be duplicated throughout the participant node.
  */
class RichSequencerClientImpl(
    domainId: DomainId,
    member: Member,
    sequencerTransports: SequencerTransports[?],
    config: SequencerClientConfig,
    testingConfig: TestingConfigInternal,
    protocolVersion: ProtocolVersion,
    domainParametersLookup: DynamicDomainParametersLookup[SequencerDomainParameters],
    timeouts: ProcessingTimeout,
    eventValidatorFactory: SequencedEventValidatorFactory,
    clock: Clock,
    requestSigner: RequestSigner,
    sequencedEventStore: SequencedEventStore,
    sendTracker: SendTracker,
    metrics: SequencerClientMetrics,
    recorderO: Option[SequencerClientRecorder],
    replayEnabled: Boolean,
    cryptoPureApi: CryptoPureApi,
    loggingConfig: LoggingConfig,
    loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
    initialCounterLowerBound: SequencerCounter,
)(implicit executionContext: ExecutionContext, tracer: Tracer)
    extends SequencerClientImpl(
      domainId,
      member,
      sequencerTransports,
      config,
      testingConfig,
      protocolVersion,
      domainParametersLookup,
      timeouts,
      eventValidatorFactory,
      clock,
      requestSigner,
      sequencedEventStore,
      sendTracker,
      metrics,
      recorderO,
      replayEnabled,
      cryptoPureApi,
      loggingConfig,
      loggerFactory,
      futureSupervisor,
      initialCounterLowerBound,
    )
    with RichSequencerClient
    with FlagCloseableAsync
    with HasFlushFuture
    with Spanning
    with HasCloseContext {

  private val sequencerAggregator =
    new SequencerAggregator(
      cryptoPureApi,
      config.eventInboxSize,
      loggerFactory,
      MessageAggregationConfig(
        sequencerTransports.expectedSequencers,
        sequencerTransports.sequencerTrustThreshold,
      ),
      timeouts,
      futureSupervisor,
    )

  sequencersTransportState.completion.onComplete { _ =>
    logger.debug(
      "The sequencer subscriptions have been closed. Closing sequencer client."
    )(TraceContext.empty)
    close()
  }

  private lazy val deferredSubscriptionHealth =
    new DelegatingMutableHealthComponent[SequencerId](
      loggerFactory,
      SequencerClient.healthName,
      timeouts,
      states =>
        SequencerAggregator
          .aggregateHealthResult(states, sequencersTransportState.getSequencerTrustThreshold),
      ComponentHealthState.failed("Disconnected from domain"),
    )

  val healthComponent: CloseableHealthComponent = deferredSubscriptionHealth

  /** Stash for storing the failure that comes out of an application handler, either synchronously or asynchronously.
    * If non-empty, no further events should be sent to the application handler.
    */
  private val applicationHandlerFailure: SingleUseCell[ApplicationHandlerFailure] =
    new SingleUseCell[ApplicationHandlerFailure]

  /** Completed iff the handler is idle. */
  private val handlerIdle: AtomicReference[Promise[Unit]] = new AtomicReference(
    Promise.successful(())
  )

  override protected def subscribeAfterInternal(
      priorTimestamp: CantonTimestamp,
      cleanPreheadTsO: Option[CantonTimestamp],
      nonThrottledEventHandler: PossiblyIgnoredApplicationHandler[ClosedEnvelope],
      timeTracker: DomainTimeTracker,
      fetchCleanTimestamp: PeriodicAcknowledgements.FetchCleanTimestamp,
      requiresAuthentication: Boolean,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val throttledEventHandler = ThrottlingApplicationEventHandler.throttle(
      config.maximumInFlightEventBatches,
      nonThrottledEventHandler,
      metrics,
    )
    val subscriptionF = performUnlessClosingUSF(functionFullName) {
      for {
        initialPriorEventO <- FutureUnlessShutdown.outcomeF(
          sequencedEventStore
            .find(SequencedEventStore.LatestUpto(priorTimestamp))
            .toOption
            .value
        )
        _ = if (initialPriorEventO.isEmpty) {
          logger.info(s"No event found up to $priorTimestamp. Resubscribing from the beginning.")
        }
        _ = cleanPreheadTsO.zip(initialPriorEventO).fold(()) {
          case (cleanPreheadTs, initialPriorEvent) =>
            ErrorUtil.requireArgument(
              initialPriorEvent.timestamp <= cleanPreheadTs,
              s"The initial prior event's timestamp ${initialPriorEvent.timestamp} is after the clean prehead at $cleanPreheadTs.",
            )
        }

        // bulk-feed the event handler with everything that we already have in the SequencedEventStore
        replayStartTimeInclusive = initialPriorEventO
          .fold(CantonTimestamp.MinValue)(_.timestamp)
          .immediateSuccessor
        _ = logger.info(
          s"Processing events from the SequencedEventStore from $replayStartTimeInclusive on"
        )

        replayEvents <- FutureUnlessShutdown.outcomeF(
          sequencedEventStore
            .findRange(
              SequencedEventStore
                .ByTimestampRange(replayStartTimeInclusive, CantonTimestamp.MaxValue),
              limit = None,
            )
            .valueOr { overlap =>
              ErrorUtil.internalError(
                new IllegalStateException(
                  s"Sequenced event store's pruning at ${overlap.pruningStatus.timestamp} is at or after the resubscription at $replayStartTimeInclusive."
                )
              )
            }
        )
        subscriptionStartsAt = replayEvents.headOption.fold(
          cleanPreheadTsO.fold(SubscriptionStart.FreshSubscription: SubscriptionStart)(
            SubscriptionStart.CleanHeadResubscriptionStart
          )
        )(replayEv =>
          SubscriptionStart.ReplayResubscriptionStart(replayEv.timestamp, cleanPreheadTsO)
        )
        _ = replayEvents.lastOption
          .orElse(initialPriorEventO)
          .foreach(event => timeTracker.subscriptionResumesAfter(event.timestamp))
        _ <- throttledEventHandler.subscriptionStartsAt(subscriptionStartsAt, timeTracker)

        eventBatches = replayEvents.grouped(config.eventInboxSize.unwrap)
        _ <- FutureUnlessShutdown.outcomeF(
          MonadUtil
            .sequentialTraverse_(eventBatches)(processEventBatch(throttledEventHandler, _))
            .valueOr(err => throw SequencerClientSubscriptionException(err))
        )
      } yield {
        val preSubscriptionEvent = replayEvents.lastOption.orElse(initialPriorEventO)
        // previously seen counter takes precedence over the lower bound
        val firstCounter = preSubscriptionEvent.fold(initialCounterLowerBound)(_.counter + 1)
        val monotonicityChecker = new SequencedEventMonotonicityChecker(
          firstCounter,
          preSubscriptionEvent.fold(CantonTimestamp.MinValue)(_.timestamp),
          loggerFactory,
        )
        val eventHandler = monotonicityChecker.handler(
          StoreSequencedEvent(sequencedEventStore, domainId, loggerFactory).apply(
            timeTracker.wrapHandler(throttledEventHandler)
          )
        )
        sequencerTransports.sequencerToTransportMap.foreach {
          case (sequencerAlias, sequencerTransport) =>
            createSubscription(
              sequencerAlias,
              sequencerTransport.sequencerId,
              preSubscriptionEvent,
              requiresAuthentication,
              eventHandler,
            ).discard
        }

        // periodically acknowledge that we've successfully processed up to the clean counter
        // We only need to it setup once; the sequencer client will direct the acknowledgements to the
        // right transport.
        if (requiresAuthentication) { // unauthenticated members don't need to ack
          periodicAcknowledgementsRef.set(
            PeriodicAcknowledgements
              .create(
                config.acknowledgementInterval.underlying,
                deferredSubscriptionHealth.getState.isOk,
                RichSequencerClientImpl.this,
                fetchCleanTimestamp,
                clock,
                timeouts,
                loggerFactory,
              )
              .some
          )
        }
      }
    }

    // we may have actually not created a subscription if we have been closed
    val loggedAbortF = subscriptionF.unwrap.map {
      case UnlessShutdown.AbortedDueToShutdown =>
        logger.info("Ignoring the sequencer subscription request as the client is being closed")
      case UnlessShutdown.Outcome(_subscription) =>
        // Everything is fine, so no need to log anything.
        ()
    }
    FutureUtil.logOnFailure(loggedAbortF, "Sequencer subscription failed")
  }

  private def createSubscription(
      sequencerAlias: SequencerAlias,
      sequencerId: SequencerId,
      preSubscriptionEvent: Option[PossiblyIgnoredSerializedEvent],
      requiresAuthentication: Boolean,
      eventHandler: OrdinaryApplicationHandler[ClosedEnvelope],
  )(implicit
      traceContext: TraceContext
  ): ResilientSequencerSubscription[SequencerClientSubscriptionError] = {
    // previously seen counter takes precedence over the lower bound
    val nextCounter = preSubscriptionEvent.fold(initialCounterLowerBound)(_.counter)
    val eventValidator = eventValidatorFactory.create(unauthenticated = !requiresAuthentication)
    logger.info(
      s"Starting subscription for alias=$sequencerAlias, id=$sequencerId at timestamp ${preSubscriptionEvent
          .map(_.timestamp)}; next counter $nextCounter"
    )

    val eventDelay: DelaySequencedEvent = {
      val first = testingConfig.testSequencerClientFor.find(elem =>
        elem.memberName == member.uid.id.unwrap &&
          elem.domainName == domainId.unwrap.id.unwrap
      )

      first match {
        case Some(value) =>
          DelayedSequencerClient.registerAndCreate(
            value.environmentId,
            domainId,
            member.uid.toString,
          )
        case None => NoDelay
      }
    }

    val subscriptionHandler = new SubscriptionHandler(
      eventHandler,
      eventValidator,
      eventDelay,
      preSubscriptionEvent,
      sequencerAlias,
      sequencerId,
    )

    val subscription = ResilientSequencerSubscription[SequencerClientSubscriptionError](
      domainId,
      protocolVersion,
      member,
      sequencersTransportState.transport(sequencerId),
      subscriptionHandler.handleEvent,
      nextCounter,
      config.initialConnectionRetryDelay.underlying,
      config.warnDisconnectDelay.underlying,
      config.maxConnectionRetryDelay.underlying,
      timeouts,
      requiresAuthentication,
      loggerFactory,
    )

    deferredSubscriptionHealth.set(sequencerId, subscription)

    sequencersTransportState
      .addSubscription(
        sequencerId,
        subscription,
        eventValidator,
      )

    // now start the subscription
    subscription.start

    subscription
  }

  private class SubscriptionHandler(
      applicationHandler: OrdinaryApplicationHandler[ClosedEnvelope],
      eventValidator: SequencedEventValidator,
      processingDelay: DelaySequencedEvent,
      initialPriorEvent: Option[PossiblyIgnoredSerializedEvent],
      sequencerAlias: SequencerAlias,
      sequencerId: SequencerId,
  ) {

    // keep track of the last event that we processed. In the event the SequencerClient is recreated or that our [[ResilientSequencerSubscription]] reconnects
    // we'll restart from the last successfully processed event counter and we'll validate it is still the last event we processed and that we're not seeing
    // a sequencer fork.
    private val priorEvent =
      new AtomicReference[Option[PossiblyIgnoredSerializedEvent]](initialPriorEvent)

    def handleEvent(
        serializedEvent: OrdinarySerializedEvent
    ): Future[Either[SequencerClientSubscriptionError, Unit]] = {
      implicit val traceContext: TraceContext = serializedEvent.traceContext
      // Process the event only if no failure has been detected
      val futureUS = applicationHandlerFailure.get.fold {
        recorderO.foreach(_.recordEvent(serializedEvent))

        // to ensure that we haven't forked since we last connected, we actually subscribe from the event we last
        // successfully processed and do another round of validations on it to ensure it's the same event we really
        // did last process. However if successful, there's no need to give it to the application handler or to store
        // it as we're really sure we've already processed it.
        // we'll also see the last event replayed if the resilient sequencer subscription reconnects.
        val isReplayOfPriorEvent = priorEvent.get().map(_.counter).contains(serializedEvent.counter)

        if (isReplayOfPriorEvent) {
          // just validate
          logger.debug(
            s"Do not handle event with sequencerCounter ${serializedEvent.counter}, as it is replayed and has already been handled."
          )
          eventValidator
            .validateOnReconnect(priorEvent.get(), serializedEvent, sequencerId)
            .leftMap[SequencerClientSubscriptionError](EventValidationError)
            .value
        } else {
          logger.debug(
            s"Validating sequenced event coming from $sequencerId (alias = $sequencerAlias) with counter ${serializedEvent.counter} and timestamp ${serializedEvent.timestamp}"
          )
          (for {
            _ <- EitherT.liftF(
              performUnlessClosingF("processing-delay")(processingDelay.delay(serializedEvent))
            )
            _ <- eventValidator
              .validate(priorEvent.get(), serializedEvent, sequencerId)
              .leftMap[SequencerClientSubscriptionError](EventValidationError)
            _ = priorEvent.set(Some(serializedEvent))

            toSignalHandler <- EitherT(
              sequencerAggregator
                .combineAndMergeEvent(
                  sequencerId,
                  serializedEvent,
                )
            )
              .leftMap[SequencerClientSubscriptionError](EventAggregationError)
          } yield
            if (toSignalHandler) {
              // TODO(#17462) Consider to remove excessive debug logging
              logger.debug(
                s"Signalling event with counter ${serializedEvent.counter} to the application handler from loop for $sequencerId (alias=$sequencerAlias)"
              )
              signalHandler(applicationHandler)
            }).value
        }
      }(err => FutureUnlessShutdown.pure(Left(err)))

      futureUS.onShutdown(Left(SequencerClientSubscriptionError.ApplicationHandlerShutdown))
    }

    // Here is how shutdown works:
    //   1. we stop injecting new events even if the handler is idle using the performUnlessClosing,
    //   2. the synchronous processing will mark handlerIdle as not completed, and once started, will be added to the flush
    //      the performUnlessClosing will guard us from entering the close method (and starting to flush) before we've successfully
    //      registered with the flush future
    //   3. once the synchronous processing finishes, it will mark the `handlerIdle` as completed and complete the flush future
    //   4. before the synchronous processing terminates and before it marks the handler to be idle again,
    //      it will add the async processing to the flush future.
    //   Consequently, on shutdown, we first have to wait on the flush future.
    //     a. No synchronous event will be added to the flush future anymore by the signalHandler
    //        due to the performUnlessClosing. Therefore, we can be sure that once the flush future terminates
    //        during shutdown, that the synchronous processing has completed and nothing new has been added.
    //     b. However, the synchronous event processing will be adding async processing to the flush future in the
    //        meantime. This means that the flush future we are waiting on might be outdated.
    //        Therefore, we have to wait on the flush future again. We can then be sure that all asynchronous
    //        futures have been added in the meantime as the synchronous flush future finished.
    //     c. I (rv) think that waiting on the `handlerIdle` is a unnecessary for shutdown as it does the
    //        same as the flush future. We only need it to ensure we don't start the sequential processing in parallel.
    private def signalHandler(
        eventHandler: OrdinaryApplicationHandler[ClosedEnvelope]
    )(implicit traceContext: TraceContext): Unit = performUnlessClosing(functionFullName) {
      val isIdle = blocking {
        synchronized {
          val oldPromise = handlerIdle.getAndUpdate(p => if (p.isCompleted) Promise() else p)
          oldPromise.isCompleted
        }
      }
      if (isIdle) {
        val handlingF = handleReceivedEventsUntilEmpty(eventHandler)
        addToFlushAndLogError("invoking the application handler")(handlingF)
      }
    }.discard

    private def handleReceivedEventsUntilEmpty(
        eventHandler: OrdinaryApplicationHandler[ClosedEnvelope]
    ): Future[Unit] = {
      val inboxSize = config.eventInboxSize.unwrap
      val javaEventList = new java.util.ArrayList[OrdinarySerializedEvent](inboxSize)
      // TODO(#17462) Consider to remove excessive debug logging
      noTracingLogger.debug(
        s"Draining events in handler loop for $sequencerId (alias=$sequencerAlias)"
      )
      if (sequencerAggregator.eventQueue.drainTo(javaEventList, inboxSize) > 0) {
        import scala.jdk.CollectionConverters.*
        val handlerEvents = javaEventList.asScala.toSeq
        // TODO(#17462) Consider to remove excessive debug logging
        noTracingLogger.debug(
          s"Drained ${handlerEvents.size} events in handler loop for $sequencerId (alias=$sequencerAlias): sequencerCounters ${handlerEvents
              .map(_.counter)
              .minOption} to ${handlerEvents.map(_.counter).maxOption}"
        )

        def stopHandler(): Unit = blocking {
          this.synchronized { val _ = handlerIdle.get().success(()) }
        }

        sendTracker
          .update(handlerEvents)
          .flatMap(_ => processEventBatch(eventHandler, handlerEvents).value)
          .transformWith {
            case Success(Right(())) => handleReceivedEventsUntilEmpty(eventHandler)
            case Success(Left(_)) | Failure(_) =>
              // `processEventBatch` has already set `applicationHandlerFailure` so we don't need to propagate the error.
              stopHandler()
              Future.unit
          }
      } else {
        val stillBusy = blocking {
          this.synchronized {
            val idlePromise = handlerIdle.get()
            if (sequencerAggregator.eventQueue.isEmpty) {
              // signalHandler must not be executed here, because that would lead to lost signals.
              idlePromise.success(())
            }
            // signalHandler must not be executed here, because that would lead to duplicate invocations.
            !idlePromise.isCompleted
          }
        }

        if (stillBusy) {
          handleReceivedEventsUntilEmpty(eventHandler)
        } else {
          // TODO(#17462) Consider to remove excessive debug logging
          noTracingLogger.debug(
            s"Stopping handler loop for $sequencerId (alias=$sequencerAlias) because we ran out of queued events"
          )
          Future.unit
        }
      }
    }
  }

  /** If the returned future fails, contains a [[scala.Left$]]
    * or [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]]
    * then [[applicationHandlerFailure]] contains an error.
    */
  private def processEventBatch[
      Box[+X <: Envelope[?]] <: PossiblyIgnoredSequencedEvent[X],
      Env <: Envelope[?],
  ](
      eventHandler: ApplicationHandler[Lambda[`+X <: Envelope[_]` => Traced[Seq[Box[X]]]], Env],
      eventBatch: Seq[Box[Env]],
  ): EitherT[Future, ApplicationHandlerFailure, Unit] =
    NonEmpty.from(eventBatch).fold(EitherT.pure[Future, ApplicationHandlerFailure](())) {
      eventBatchNE =>
        applicationHandlerFailure.get.fold {
          implicit val batchTraceContext: TraceContext = TraceContext.ofBatch(eventBatch)(logger)
          val lastSc = eventBatchNE.last1.counter
          val firstEvent = eventBatchNE.head1
          val firstSc = firstEvent.counter
          metrics.handler.numEvents.inc(eventBatch.size.toLong)(MetricsContext.Empty)
          logger.debug(
            s"Passing ${eventBatch.size} events to the application handler ${eventHandler.name}."
          )
          // Measure only the synchronous part of the application handler so that we see how much the application handler
          // contributes to the sequential processing bottleneck.
          val asyncResultFT =
            Try(
              Timed
                .future(metrics.handler.applicationHandle, eventHandler(Traced(eventBatch)).unwrap)
            )

          def putApplicationHandlerFailure(
              failure: ApplicationHandlerFailure
          ): ApplicationHandlerFailure = {
            val alreadyCompleted = applicationHandlerFailure.putIfAbsent(failure)
            alreadyCompleted.foreach { earlierFailure =>
              logger.debug(show"Another event processing has previously failed: $earlierFailure")
            }
            logger.debug("Clearing the receivedEvents queue to unblock the subscription.")
            // Clear the receivedEvents queue, because the thread that inserts new events to the queue may block.
            // Clearing the queue is potentially dangerous, because it may result in data loss.
            // To prevent that, clear the queue only after setting applicationHandlerFailure.
            // - Once the applicationHandlerFailure has been set, any subsequent invocations of this method won't invoke
            //   the application handler.
            // - Ongoing invocations of this method are not affected by clearing the queue,
            //   because the events processed by the ongoing invocation have been drained from the queue before clearing.
            sequencerAggregator.eventQueue.clear()
            failure
          }

          def handleException(
              error: Throwable,
              syncProcessing: Boolean,
          ): ApplicationHandlerFailure = {
            val sync = if (syncProcessing) "Synchronous" else "Asynchronous"

            error match {
              case PassiveInstanceException(reason) =>
                logger.warn(
                  s"$sync event processing stopped because instance became passive"
                )
                putApplicationHandlerFailure(ApplicationHandlerPassive(reason))

              case _ if isClosing =>
                logger.info(
                  s"$sync event processing failed for event batch with sequencer counters $firstSc to $lastSc, most likely due to an ongoing shutdown",
                  error,
                )
                putApplicationHandlerFailure(ApplicationHandlerShutdown)

              case _ =>
                logger.error(
                  s"$sync event processing failed for event batch with sequencer counters $firstSc to $lastSc.",
                  error,
                )
                putApplicationHandlerFailure(ApplicationHandlerException(error, firstSc, lastSc))
            }
          }

          def handleAsyncResult(
              asyncResultF: Future[UnlessShutdown[AsyncResult]]
          ): EitherT[Future, ApplicationHandlerFailure, Unit] =
            EitherTUtil
              .fromFuture(asyncResultF, handleException(_, syncProcessing = true))
              .subflatMap {
                case UnlessShutdown.Outcome(asyncResult) =>
                  val asyncSignalledF = asyncResult.unwrap.transformIntoSuccess { result =>
                    // record errors and shutdown in `applicationHandlerFailure` and move on
                    result match {
                      case Success(outcome) =>
                        outcome
                          .onShutdown(
                            putApplicationHandlerFailure(ApplicationHandlerShutdown).discard
                          )
                          .discard
                      case Failure(error) =>
                        handleException(error, syncProcessing = false).discard
                    }
                    UnlessShutdown.unit
                  }.unwrap
                  // note, we are adding our async processing to the flush future, so we know once the async processing has finished
                  addToFlushAndLogError(
                    s"asynchronous event processing for event batch with sequencer counters $firstSc to $lastSc"
                  )(asyncSignalledF)
                  // we do not wait for the async results to finish, we are done here once the synchronous part is done
                  Right(())
                case UnlessShutdown.AbortedDueToShutdown =>
                  putApplicationHandlerFailure(ApplicationHandlerShutdown).discard
                  Left(ApplicationHandlerShutdown)
              }

          // note, here, we created the asyncResultF, which means we've completed the synchronous processing part.
          asyncResultFT.fold(
            error => EitherT.leftT[Future, Unit](handleException(error, syncProcessing = true)),
            handleAsyncResult,
          )
        }(EitherT.leftT[Future, Unit](_))
    }

  def changeTransport(
      sequencerTransports: SequencerTransports[?]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    sequencerAggregator.changeMessageAggregationConfig(
      MessageAggregationConfig(
        sequencerTransports.expectedSequencers,
        sequencerTransports.sequencerTrustThreshold,
      )
    )
    sequencersTransportState.changeTransport(sequencerTransports)
  }

  /** Future which is completed when the client is not functional any more and is ready to be closed.
    * The value with which the future is completed will indicate the reason for completion.
    */
  def completion: Future[SequencerClient.CloseReason] = sequencersTransportState.completion

  private def waitForHandlerToComplete(): Unit = {
    import TraceContext.Implicits.Empty.*
    logger.trace(s"Wait for the handler to become idle")
    // This logs a warn if the handle does not become idle within 60 seconds.
    // This happen because the handler is not making progress, for example due to a db outage.
    valueOrLog(
      handlerIdle.get().future,
      timeoutMessage = s"Clean close of the sequencer subscriptions timed out",
      timeout = timeouts.shutdownProcessing.unwrap,
    ).discard
  }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    Seq(
      SyncCloseable("sequencer-aggregator", sequencerAggregator.close()),
      SyncCloseable("sequencer-send-tracker", sendTracker.close()),
      // see comments above why we need two flushes
      flushCloseable("sequencer-client-flush-sync", timeouts.shutdownProcessing),
      flushCloseable("sequencer-client-flush-async", timeouts.shutdownProcessing),
      SyncCloseable("sequencer-client-subscription", sequencersTransportState.close()),
      SyncCloseable("handler-becomes-idle", waitForHandlerToComplete()),
      SyncCloseable(
        "sequencer-client-periodic-ack",
        toCloseableOption(periodicAcknowledgementsRef.get()).close(),
      ),
      SyncCloseable("sequencer-client-recorder", recorderO.foreach(_.close())),
      SyncCloseable("deferred-subscription-health", deferredSubscriptionHealth.close()),
    )
  }

  /** Returns a future that completes after asynchronous processing has completed for all events
    * whose synchronous processing has been completed prior to this call. May complete earlier if event processing
    * has failed.
    */
  @VisibleForTesting
  def flush(): Future[Unit] = doFlush()

  /** Await the completion of `future`. Log a message if the future does not complete within `timeout`.
    * If the `future` fails with an exception within `timeout`, this method rethrows the exception.
    *
    * Instead of using this method, you should use the respective method on one of the ProcessingTimeouts
    *
    * @return Optionally the completed value of `future` if it successfully completes in time.
    */
  private def valueOrLog[T](
      future: Future[T],
      timeoutMessage: => String,
      timeout: Duration,
      level: Level = Level.WARN,
      stackTraceFilter: Thread => Boolean = defaultStackTraceFilter,
  )(implicit loggingContext: ErrorLoggingContext): Option[T] = {
    // Use Await.ready instead of Await.result to be able to tell the difference between the awaitable throwing a
    // TimeoutException and a TimeoutException being thrown because the awaitable is not ready.
    val ready = Try(Await.ready(future, timeout))
    ready match {
      case Success(awaited) =>
        val result = awaited.value.getOrElse(
          throw new RuntimeException(s"Future $future not completed after successful Await.ready.")
        )
        result.fold(throw _, Some(_))

      case Failure(timeoutExc: TimeoutException) =>
        val stackTraces = StackTraceUtil.formatStackTrace(stackTraceFilter)
        if (stackTraces.isEmpty)
          LoggerUtil.logThrowableAtLevel(level, timeoutMessage, timeoutExc)
        else
          LoggerUtil.logThrowableAtLevel(
            level,
            s"$timeoutMessage\nStack traces:\n$stackTraces",
            timeoutExc,
          )
        None

      case Failure(exc) => ErrorUtil.internalError(exc)
    }
  }
}

class SequencerClientImplPekko[E: Pretty](
    domainId: DomainId,
    member: Member,
    sequencerTransports: SequencerTransports[E],
    config: SequencerClientConfig,
    testingConfig: TestingConfigInternal,
    protocolVersion: ProtocolVersion,
    domainParametersLookup: DynamicDomainParametersLookup[SequencerDomainParameters],
    timeouts: ProcessingTimeout,
    eventValidatorFactory: SequencedEventValidatorFactory,
    clock: Clock,
    requestSigner: RequestSigner,
    sequencedEventStore: SequencedEventStore,
    sendTracker: SendTracker,
    metrics: SequencerClientMetrics,
    recorderO: Option[SequencerClientRecorder],
    replayEnabled: Boolean,
    cryptoPureApi: CryptoPureApi,
    loggingConfig: LoggingConfig,
    loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
    initialCounterLowerBound: SequencerCounter,
)(implicit executionContext: ExecutionContext, tracer: Tracer, materializer: Materializer)
    extends SequencerClientImpl(
      domainId,
      member,
      sequencerTransports,
      config,
      testingConfig,
      protocolVersion,
      domainParametersLookup,
      timeouts,
      eventValidatorFactory,
      clock,
      requestSigner,
      sequencedEventStore,
      sendTracker,
      metrics,
      recorderO,
      replayEnabled,
      cryptoPureApi,
      loggingConfig,
      loggerFactory,
      futureSupervisor,
      initialCounterLowerBound,
    ) {

  import SequencerClientImplPekko.*

  private val subscriptionHandle: AtomicReference[Option[SubscriptionHandle]] =
    new AtomicReference[Option[SubscriptionHandle]](None)

  override protected def subscribeAfterInternal(
      priorTimestamp: CantonTimestamp,
      cleanPreheadTsO: Option[CantonTimestamp],
      nonThrottledEventHandler: PossiblyIgnoredApplicationHandler[ClosedEnvelope],
      timeTracker: DomainTimeTracker,
      fetchCleanTimestamp: FetchCleanTimestamp,
      requiresAuthentication: Boolean,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val throttledEventHandler = ThrottlingApplicationEventHandler.throttle(
      config.maximumInFlightEventBatches,
      nonThrottledEventHandler,
      metrics,
    )
    val subscriptionF = performUnlessClosingUSF(functionFullName) {
      for {
        initialPriorEventO <- FutureUnlessShutdown.outcomeF(
          sequencedEventStore
            .find(SequencedEventStore.LatestUpto(priorTimestamp))
            .toOption
            .value
        )
        _ = if (initialPriorEventO.isEmpty) {
          logger.info(s"No event found up to $priorTimestamp. Resubscribing from the beginning.")
        }
        _ = cleanPreheadTsO.zip(initialPriorEventO).fold(()) {
          case (cleanPreheadTs, initialPriorEvent) =>
            ErrorUtil.requireArgument(
              initialPriorEvent.timestamp <= cleanPreheadTs,
              s"The initial prior event's timestamp ${initialPriorEvent.timestamp} is after the clean prehead at $cleanPreheadTs.",
            )
        }

        // bulk-feed the event handler with everything that we already have in the SequencedEventStore
        replayStartTimeInclusive = initialPriorEventO
          .fold(CantonTimestamp.MinValue)(_.timestamp)
          .immediateSuccessor
        _ = logger.info(
          s"Processing events from the SequencedEventStore from ${replayStartTimeInclusive} on"
        )

        replayEvents <- FutureUnlessShutdown.outcomeF(
          sequencedEventStore
            .findRange(
              SequencedEventStore
                .ByTimestampRange(replayStartTimeInclusive, CantonTimestamp.MaxValue),
              limit = None,
            )
            .valueOr { overlap =>
              ErrorUtil.internalError(
                new IllegalStateException(
                  s"Sequenced event store's pruning at ${overlap.pruningStatus.timestamp} is at or after the resubscription at $replayStartTimeInclusive."
                )
              )
            }
        )
        subscriptionStartsAt = replayEvents.headOption.fold(
          cleanPreheadTsO.fold(SubscriptionStart.FreshSubscription: SubscriptionStart)(
            SubscriptionStart.CleanHeadResubscriptionStart
          )
        )(replayEv =>
          SubscriptionStart.ReplayResubscriptionStart(replayEv.timestamp, cleanPreheadTsO)
        )
        _ = replayEvents.lastOption
          .orElse(initialPriorEventO)
          .foreach(event => timeTracker.subscriptionResumesAfter(event.timestamp))
        _ <- throttledEventHandler.subscriptionStartsAt(subscriptionStartsAt, timeTracker)
      } yield {
        val preSubscriptionEvent = replayEvents.lastOption.orElse(initialPriorEventO)
        // previously seen counter takes precedence over the lower bound
        val firstCounter = preSubscriptionEvent.fold(initialCounterLowerBound)(_.counter)
        val initialCounterOrPriorEvent = preSubscriptionEvent.toRight(firstCounter)
        lazy val subscriptionStartLogMessage = initialCounterOrPriorEvent match {
          case Left(counter) =>
            s"Subscription starts without prior event at counter $counter"
          case Right(event) =>
            s"Subscription starts at prior event at ${event.timestamp} with counter ${event.counter}"
        }
        logger.debug(subscriptionStartLogMessage)

        val eventValidator = eventValidatorFactory.create(unauthenticated = !requiresAuthentication)
        val aggregator = new SequencerAggregatorPekko(
          domainId,
          eventValidator,
          bufferSize = PositiveInt.one,
          cryptoPureApi,
          loggerFactory,
          // TODO(#13789) wire this up
          enableInvariantCheck = false,
        )

        val replayCompleted = mkPromise[Unit]("replay-of-sequenced-events", futureSupervisor)

        val batchedReplayedEvents = replayEvents
          .grouped(config.eventInboxSize.unwrap)
          .map { batch =>
            val batchTraceContext = TraceContext.ofBatch(batch)(logger)
            WithPromise(Traced(batch)(batchTraceContext))()
          }
          .toSeq
        // Zip together all the completions of the replayed events
        val replayCompletion = batchedReplayedEvents.parTraverse_ { withPromise =>
          withPromise.promise.future
        }
        replayCompleted.completeWith(FutureUnlessShutdown.outcomeF(replayCompletion))

        val replayedEventsSource =
          Source(batchedReplayedEvents).map(Right(_)).viaMat(KillSwitches.single)(Keep.right)

        val sequencerConnectionConfig =
          OrderedBucketMergeConfig[SequencerId, HasSequencerSubscriptionFactoryPekko[E]](
            sequencerTransports.sequencerTrustThreshold,
            sequencerTransports.sequencerIdToTransportMap.toNEF.fmap { transportContainer =>
              SequencerSubscriptionFactoryPekko.fromTransport(
                transportContainer.sequencerId,
                transportContainer.clientTransport,
                requiresAuthentication,
                member,
                protocolVersion,
              )
            }.fromNEF,
          )

        val configSource = Source
          .single(sequencerConnectionConfig)
          .concat(Source.never)
          .viaMat(KillSwitches.single)(Keep.right)

        val monotonicityChecker = new SequencedEventMonotonicityChecker(
          firstCounter,
          preSubscriptionEvent.fold(CantonTimestamp.MinValue)(_.timestamp),
          loggerFactory,
        )
        val storeSequencedEvent = StoreSequencedEvent(sequencedEventStore, domainId, loggerFactory)

        val aggregatorFlow = aggregator.aggregateFlow(initialCounterOrPriorEvent)
        val subscriptionSource = configSource
          .viaMat(aggregatorFlow)(Keep.both)
          .injectKillSwitch { case (killSwitch, _) => killSwitch }
          .via(monotonicityChecker.flow)
          .map(_.value)
          // Drop the first event if it's a resubscription because we don't want to pass it to the application handler any more
          .via(dropPriorEvent(preSubscriptionEvent.isDefined))
          .via(batchFlow)
          .mapAsync(parallelism = 1) { controlOrEvent =>
            controlOrEvent.traverse(tracedEvents =>
              sendTracker.update(tracedEvents.value).map((_: Unit) => tracedEvents)
            )
          }
          .map(_.map(eventBatch => WithPromise(eventBatch)()))

        type F1[+A] = Either[SubscriptionControl[E], WithPromise[A]]
        implicit val singletonTraverseT: SingletonTraverse.Aux[F1, Promise[Unit]] =
          SingletonTraverse[Either[SubscriptionControl[E], *]]
            .composeWith(SingletonTraverse[WithPromise])(Keep.right)
        val persistedSubscriptionSource = subscriptionSource
          .via(storeSequencedEvent.flow[F1])
          .via(timeTracker.flow[F1, ClosedEnvelope])

        val eventSource: Source[
          Either[SubscriptionControl[E], WithPromise[Traced[
            Seq[PossiblyIgnoredSerializedEvent]
          ]]],
          (KillSwitch, Future[Done], HealthComponent),
        ] = replayedEventsSource.concatLazyMat(persistedSubscriptionSource) {
          (replayedKillSwitch, subscriptionMat) =>
            val (subscriptionKillSwitch, (doneF, health)) = subscriptionMat
            val combinedKillSwitch =
              new CombinedKillSwitch(replayedKillSwitch, subscriptionKillSwitch)
            (combinedKillSwitch, doneF, health)
        }

        type F2[+X] = WithKillSwitch[F1[X]]
        implicit val singletonTraverseF: SingletonTraverse.Aux[F2, (KillSwitch, Promise[Unit])] =
          SingletonTraverse[WithKillSwitch].composeWith(SingletonTraverse[F1])(Keep.both)

        val applicationHandlerPekko = new ApplicationHandlerPekko[F2, (KillSwitch, Promise[Unit])](
          throttledEventHandler,
          metrics,
          loggerFactory,
          { case (killSwitch, _) => killSwitch },
        )

        val stream = eventSource
          .injectKillSwitch { case (killSwitch, _, _) => killSwitch }
          .via(applicationHandlerPekko.asFlow(config.maximumInFlightEventBatches))
          // Mark that the application handler has finished a given batch of promises.
          .map(_.map(_.map { withPromise =>
            val completion = withPromise.value match {
              case Outcome(Left(error)) => Failure(SequencerClientSubscriptionException(error))
              case _ => Success(())
            }
            withPromise.promise.tryComplete(completion).discard[Boolean]
            withPromise
          }))
          .collect {
            // Discard AbortedDueToShutdown and normal asynchronous results:
            // AbortedDueToShutdown may originate from asynchronous processing,
            // but the root cause of the shutdown could be a failure in the synchronous processing,
            // possibly for a later event.
            // So a regular shutdown will appear as a normal completion of the stream.
            //
            // Also discard control messages (Left).
            //  TODO(#13789) This may change when we support dynamic transport changes
            case WithKillSwitch(Right(WithPromise(Outcome(Left(error))))) =>
              error
          }
          .toMat(Sink.lastOption) { (matEventSource, lastF) =>
            val extractedFailureF = lastF.map {
              case None =>
                logger.debug("sequencer subscription stream terminated normally")
                AbortedDueToShutdown
              case Some(error) =>
                logger.debug(s"sequencer subscription stream terminated abnormally: $error")
                Outcome(error)
            }
            matEventSource -> FutureUnlessShutdown(extractedFailureF)
          }

        val ((killSwitch, subscriptionDoneF, health), completion) =
          PekkoUtil.runSupervised(logger.error("Sequencer subscription failed", _), stream)
        val handle = SubscriptionHandle(killSwitch, subscriptionDoneF, completion)
        subscriptionHandle.getAndSet(Some(handle)).foreach { _ =>
          // TODO(#13789) Clean up the error logging.
          //  Currently, this mimics com.digitalasset.canton.sequencing.client.SequencersTransportState.addSubscription
          logger.warn(
            "Cannot create additional subscriptions to the sequencer from the same client"
          )
          throw new IllegalArgumentException(
            s"The sequencer client already has a running subscription"
          )
        }

        // periodically acknowledge that we've successfully processed up to the clean counter
        // We only need to it setup once; the sequencer client will direct the acknowledgements to the
        // right transport.
        if (requiresAuthentication) { // unauthenticated members don't need to ack
          periodicAcknowledgementsRef.set(
            PeriodicAcknowledgements
              .create(
                config.acknowledgementInterval.underlying,
                health.getState.isOk,
                this,
                fetchCleanTimestamp,
                clock,
                timeouts,
                loggerFactory,
              )
              .some
          )
        }

        replayCompleted.futureUS
      }
    }
    // we may have actually not created a subscription if we have been closed
    val loggedAbortF = subscriptionF.flatten.onShutdown {
      logger.info("Ignoring the sequencer subscription request as the client is being closed")
    }

    FutureUtil.logOnFailure(loggedAbortF, "Sequencer subscription failed")
  }

  private def dropPriorEvent[A, B](doDrop: Boolean): Flow[Either[A, B], Either[A, B], NotUsed] =
    if (doDrop) Flow[Either[A, B]].dropIf(1)(_.isRight)
    else Flow[Either[A, B]]

  private def batchFlow[A, B <: HasTraceContext](implicit
      traceContext: TraceContext
  ): Flow[Either[A, B], Either[A, Traced[Seq[B]]], NotUsed] =
    Flow[Either[A, B]]
      .batchN(config.eventInboxSize.unwrap, 1)
      .mapConcat { batchBuffer =>
        val batchesOrError =
          IterableUtil.spansBy(batchBuffer.toSeq)(_.isRight).flatMap { case (_, block) =>
            val (lefts, rights) = block.forgetNE.separate
            ErrorUtil.requireState(
              lefts.isEmpty || rights.isEmpty,
              "spansBy returned Lefts and Rights in the same block",
            )
            if (lefts.isEmpty) {
              val batchTraceContext = TraceContext.ofBatch(rights)(logger)
              Seq(Right(Traced(rights)(batchTraceContext)))
            } else lefts.map(left => Left(left))
          }
        batchesOrError
      }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    subscriptionHandle.get.toList.flatMap { handle =>
      import com.digitalasset.canton.tracing.TraceContext.Implicits.Empty.*

      Seq(
        SyncCloseable("subscription kill switch", handle.killSwitch.shutdown()),
        AsyncCloseable(
          "subscription completion",
          handle.sourceCompletion,
          timeouts.shutdownProcessing,
        ),
        AsyncCloseable(
          "application handler completion",
          handle.applicationHandlerCompletion.unwrap,
          timeouts.shutdownProcessing,
        ),
      )
    }
  }
}

object SequencerClientImplPekko {
  private final case class WithPromise[+A](override val value: A)(
      val promise: Promise[Unit] = Promise[Unit]()
  ) extends WithGeneric[A, Promise[Unit], WithPromise] {
    override protected def added: Promise[Unit] = promise
    override protected def update[AA](value: AA): WithPromise[AA] = copy(value)(promise)
  }
  private object WithPromise extends WithGenericCompanion {
    implicit val singletonTraverseWithPromise: SingletonTraverse.Aux[WithPromise, Promise[Unit]] =
      singletonTraverseWithGeneric[Promise[Unit], WithPromise]
  }

  private final case class SubscriptionHandle(
      killSwitch: KillSwitch,
      sourceCompletion: Future[Done],
      applicationHandlerCompletion: FutureUnlessShutdown[ApplicationHandlerError],
  )

  private sealed trait BatchCounterRange extends Product with Serializable with PrettyPrinting
  private case object EmptyBatch extends BatchCounterRange {
    override def pretty: Pretty[EmptyBatch.type] = prettyOfObject[EmptyBatch.type]
  }
  private final case class NonEmptyBatchCounterRange(start: SequencerCounter, end: SequencerCounter)
      extends BatchCounterRange {
    override def pretty: Pretty[NonEmptyBatchCounterRange] = prettyInfix(_.start, "->", _.end)
  }
  private object BatchCounterRange {
    def apply(
        batch: Traced[Seq[PossiblyIgnoredSerializedEvent]]
    ): BatchCounterRange =
      NonEmpty.from(batch.value) match {
        case None => EmptyBatch
        case Some(batchNE) =>
          NonEmptyBatchCounterRange(batchNE.head1.counter, batchNE.last1.counter)
      }
  }
}

object SequencerClient {
  val healthName: String = "sequencer-client"

  final case class SequencerTransportContainer[E](
      sequencerId: SequencerId,
      clientTransport: SequencerClientTransport & SequencerClientTransportPekko.Aux[E],
  )

  final case class SequencerTransports[E](
      sequencerToTransportMap: NonEmpty[Map[SequencerAlias, SequencerTransportContainer[E]]],
      sequencerTrustThreshold: PositiveInt,
      submissionRequestAmplification: PositiveInt,
  ) {
    def expectedSequencers: NonEmpty[Set[SequencerId]] =
      sequencerToTransportMap.map(_._2.sequencerId).toSet

    def sequencerIdToTransportMap: NonEmpty[Map[SequencerId, SequencerTransportContainer[E]]] = {
      sequencerToTransportMap.map { case (_, transport) =>
        transport.sequencerId -> transport
      }.toMap
    }

    def transports: Set[SequencerClientTransport] =
      sequencerToTransportMap.values.map(_.clientTransport).toSet
  }

  object SequencerTransports {
    def from[E](
        sequencerTransportsMap: NonEmpty[
          Map[SequencerAlias, SequencerClientTransport & SequencerClientTransportPekko.Aux[E]]
        ],
        expectedSequencers: NonEmpty[Map[SequencerAlias, SequencerId]],
        sequencerSignatureThreshold: PositiveInt,
        submissionRequestAmplification: PositiveInt,
    ): Either[String, SequencerTransports[E]] =
      if (sequencerTransportsMap.keySet != expectedSequencers.keySet) {
        Left("Inconsistent map of sequencer transports and their ids.")
      } else
        Right(
          SequencerTransports(
            sequencerToTransportMap =
              sequencerTransportsMap.map { case (sequencerAlias, transport) =>
                val sequencerId = expectedSequencers(sequencerAlias)
                sequencerAlias -> SequencerTransportContainer(sequencerId, transport)
              }.toMap,
            sequencerTrustThreshold = sequencerSignatureThreshold,
            submissionRequestAmplification = submissionRequestAmplification,
          )
        )

    def single[E](
        sequencerAlias: SequencerAlias,
        sequencerId: SequencerId,
        transport: SequencerClientTransport & SequencerClientTransportPekko.Aux[E],
    ): SequencerTransports[E] =
      SequencerTransports(
        NonEmpty
          .mk(
            Seq,
            sequencerAlias -> SequencerTransportContainer(sequencerId, transport),
          )
          .toMap,
        PositiveInt.one,
        PositiveInt.one,
      )

    def default[E](
        sequencerId: SequencerId,
        transport: SequencerClientTransport & SequencerClientTransportPekko.Aux[E],
    ): SequencerTransports[E] =
      single(SequencerAlias.Default, sequencerId, transport)
  }

  sealed trait CloseReason

  object CloseReason {

    trait ErrorfulCloseReason

    final case class PermissionDenied(cause: String) extends CloseReason

    final case class UnrecoverableError(cause: String) extends ErrorfulCloseReason with CloseReason

    final case class UnrecoverableException(throwable: Throwable)
        extends ErrorfulCloseReason
        with CloseReason

    case object ClientShutdown extends CloseReason

    case object BecamePassive extends CloseReason
  }

  /** Hook for informing tests about replay statistics.
    *
    * If a [[SequencerClient]] is used with
    * [[transports.replay.ReplayingEventsSequencerClientTransport]], the transport
    * will add a statistics to this queue whenever a replay attempt has completed successfully.
    *
    * A test can poll this statistics from the queue to determine whether the replay has completed and to
    * get statistics on the replay.
    *
    * LIMITATION: This is only suitable for manual / sequential test setups, as the statistics are shared through
    * a global queue.
    */
  @VisibleForTesting
  lazy val replayStatistics: BlockingQueue[ReplayStatistics] = new LinkedBlockingQueue()

  final case class ReplayStatistics(
      inputPath: Path,
      numberOfEvents: Int,
      startTime: CantonTimestamp,
      duration: JDuration,
  )

  /** Utility to add retries around sends as an attempt to guarantee the send is eventually sequenced.
    */
  def sendWithRetries(
      sendBatch: SendCallback => EitherT[Future, SendAsyncClientError, Unit],
      maxRetries: Int,
      delay: FiniteDuration,
      sendDescription: String,
      errMsg: String,
      performUnlessClosing: PerformUnlessClosing,
  )(implicit
      ec: ExecutionContext,
      loggingContext: ErrorLoggingContext,
  ): FutureUnlessShutdown[Unit] = {
    def doSend(): FutureUnlessShutdown[Unit] = {
      val callback = new CallbackFuture()
      for {
        _ <- FutureUnlessShutdown
          .outcomeF(
            EitherTUtil.toFuture(
              EitherTUtil
                .logOnError(sendBatch(callback), errMsg)
                .leftMap(err => new RuntimeException(s"$errMsg: $err"))
            )
          )
        sendResult <- callback.future
        _ <- SendResult.toFutureUnlessShutdown(sendDescription)(sendResult)
      } yield ()
    }
    retry
      .Pause(loggingContext.logger, performUnlessClosing, maxRetries, delay, sendDescription)
      .unlessShutdown(doSend(), AllExnRetryable)(
        retry.Success.always,
        ec,
        loggingContext.traceContext,
      )
  }
}
