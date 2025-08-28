// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{CryptoPureApi, Hash, HashPurpose, Signature}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.health.ComponentHealthState
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  HasRunOnClosing,
  PromiseUnlessShutdown,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.SequencerAggregator.{
  MessageAggregationConfig,
  SequencerAggregatorError,
}
import com.digitalasset.canton.sequencing.SequencerSubscriptionPoolImpl.SubscriptionStartProvider
import com.digitalasset.canton.sequencing.protocol.SignedContent
import com.digitalasset.canton.store.SequencedEventStore.SequencedEventWithTraceContext
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil.*
import com.google.common.annotations.VisibleForTesting
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, blocking}

class SequencerAggregator(
    cryptoPureApi: CryptoPureApi,
    eventInboxSize: PositiveInt,
    val loggerFactory: NamedLoggerFactory,
    initialConfig: MessageAggregationConfig,
    updateSendTracker: Seq[SequencedEventWithTraceContext[?]] => Unit,
    override val timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    useNewConnectionPool: Boolean,
) extends SubscriptionStartProvider
    with NamedLogging
    with FlagCloseable {

  private val postAggregationHandlerRef = new AtomicReference[Option[PostAggregationHandler]](None)
  def setPostAggregationHandler(postAggregationHandler: PostAggregationHandler): Unit =
    postAggregationHandlerRef
      .getAndSet(Some(postAggregationHandler))
      .foreach(_ => throw new IllegalStateException("Post aggregation handler already set"))

  private val configRef: AtomicReference[MessageAggregationConfig] =
    new AtomicReference[MessageAggregationConfig](initialConfig)
  def expectedSequencers: NonEmpty[Set[SequencerId]] = configRef
    .get()
    .expectedSequencersO
    .getOrElse(
      throw new IllegalStateException(
        "Missing `expectedSequencers`: called while using the connection pool?"
      )
    )

  def sequencerTrustThreshold: PositiveInt = configRef.get().sequencerTrustThreshold

  private case class SequencerMessageData(
      eventBySequencer: Map[SequencerId, SequencedSerializedEvent],
      promise: PromiseUnlessShutdown[Either[SequencerAggregatorError, SequencerId]],
  )

  /** Queue containing received and not yet handled events. Used for batched processing.
    */
  private val receivedEvents: BlockingQueue[SequencedSerializedEvent] =
    new ArrayBlockingQueue[SequencedSerializedEvent](eventInboxSize.unwrap)

  private val sequenceData = mutable.TreeMap.empty[CantonTimestamp, SequencerMessageData]

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var cursor: Option[CantonTimestamp] = None

  private val latestAggregatedEventRef = new AtomicReference[Option[SequencedSerializedEvent]](None)

  override def getLatestProcessedEventO: Option[SequencedSerializedEvent] =
    latestAggregatedEventRef.get

  def eventQueue: BlockingQueue[SequencedSerializedEvent] = receivedEvents

  private def hash(message: SequencedSerializedEvent) =
    SignedContent.hashContent(
      cryptoPureApi,
      message.signedEvent.content,
      HashPurpose.SequencedEventSignature,
    )

  @VisibleForTesting
  def combine(
      messages: NonEmpty[Seq[SequencedSerializedEvent]]
  ): Either[SequencerAggregatorError, SequencedSerializedEvent] = {
    val message: SequencedSerializedEvent = messages.head1
    val expectedMessageHash = hash(message)
    val hashes: NonEmpty[Set[Hash]] = messages.map(hash).toSet
    for {
      _ <- Either.cond(
        hashes.forall(_ == expectedMessageHash),
        (),
        SequencerAggregatorError.NotTheSameContentHash(hashes),
      )
    } yield {
      val combinedSignatures: NonEmpty[Seq[Signature]] = messages.flatMap(_.signedEvent.signatures)

      val potentiallyNonEmptyTraceContext = messages
        .find(_.traceContext != TraceContext.empty)
        .map(_.traceContext)
        .getOrElse(message.traceContext)

      SequencedEventWithTraceContext(message.signedEvent.copy(signatures = combinedSignatures))(
        potentiallyNonEmptyTraceContext
      )
    }
  }

  private def addEventToQueue(event: SequencedSerializedEvent): Unit = {
    implicit val traceContext: TraceContext = event.traceContext
    logger.debug(
      show"Storing event in the event inbox.\n${event.signedEvent.content}"
    )

    updateSendTracker(Seq(event))

    latestAggregatedEventRef.set(Some(event))
    if (!receivedEvents.offer(event)) {
      logger.info(
        s"Event inbox is full. Blocking sequenced event with timestamp ${event.timestamp}."
      )
      blocking {
        receivedEvents.put(event)
      }
      logger.info(
        s"Unblocked sequenced event with timestamp ${event.timestamp}."
      )
    }

    if (useNewConnectionPool) {
      logger.debug("Signalling the application handler")
      postAggregationHandlerRef.get
        .getOrElse(ErrorUtil.invalidState("Missing post aggregation handler"))
        .signalHandler()
    }
  }

  private def addEventToQueue(
      messages: NonEmpty[List[SequencedSerializedEvent]]
  ): Either[SequencerAggregatorError, Unit] =
    combine(messages).map(addEventToQueue)

  @SuppressWarnings(Array("com.digitalasset.canton.SynchronizedFuture"))
  def combineAndMergeEvent(
      sequencerId: SequencerId,
      message: SequencedSerializedEvent,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Either[SequencerAggregatorError, Boolean]] =
    // The reason why this was checked here is unclear. The SequencedEventValidator already checks that
    // events come from valid sequencers by verifying the signature using up-to-date topology state.
    if (!useNewConnectionPool && !expectedSequencers.contains(sequencerId)) {
      FutureUnlessShutdown(
        ErrorUtil.internalErrorAsync(
          new IllegalArgumentException(s"Unexpected sequencerId: $sequencerId")
        )
      )
    } else
      try {
        blocking {
          this.synchronized {
            if (cursor.forall(message.timestamp > _)) {
              val sequencerMessageData = updatedSequencerMessageData(sequencerId, message)
              sequenceData.put(message.timestamp, sequencerMessageData).discard

              val (nextMinimumTimestamp, nextData) =
                sequenceData.headOption.getOrElse(
                  (message.timestamp, sequencerMessageData)
                ) // returns min message.timestamp

              pushDownstreamIfConsensusIsReached(nextMinimumTimestamp, nextData)

              sequencerMessageData.promise.futureUS.map(_.map(_ == sequencerId))
            } else
              FutureUnlessShutdown.pure(Right(false))
          }
        }
      } catch {
        case t: Throwable =>
          logger.error("Error while combining and merging event", t)
          FutureUnlessShutdown.failed(t)
      }

  private def pushDownstreamIfConsensusIsReached(
      nextMinimumTimestamp: CantonTimestamp,
      nextData: SequencerMessageData,
  ): Unit = {
    val expectedMessages =
      if (useNewConnectionPool) nextData.eventBySequencer
      else
        nextData.eventBySequencer.view.filterKeys { sequencerId =>
          expectedSequencers.contains(sequencerId)
        }.toMap

    if (expectedMessages.sizeCompare(sequencerTrustThreshold.unwrap) >= 0) {
      cursor = Some(nextMinimumTimestamp)
      sequenceData.remove(nextMinimumTimestamp).discard

      val nonEmptyMessages = NonEmptyUtil.fromUnsafe(expectedMessages)
      val messagesToCombine = nonEmptyMessages.map { case (_, event) => event }.toList
      val (sequencerIdToNotify, _) = nonEmptyMessages.head1

      nextData.promise
        .outcome_(
          addEventToQueue(messagesToCombine).map(_ => sequencerIdToNotify)
        )
    }
  }

  private def updatedSequencerMessageData(
      sequencerId: SequencerId,
      message: SequencedSerializedEvent,
  ): SequencerMessageData = {
    implicit val traceContext: TraceContext = message.traceContext
    val promise = PromiseUnlessShutdown.supervised[Either[SequencerAggregatorError, SequencerId]](
      "replica-manager-sync-service",
      futureSupervisor,
    )
    val data =
      sequenceData.getOrElse(
        message.timestamp,
        SequencerMessageData(Map(), promise),
      )
    data.copy(eventBySequencer = data.eventBySequencer.updated(sequencerId, message))
  }

  def changeMessageAggregationConfig(
      newConfig: MessageAggregationConfig
  ): Unit = blocking {
    this.synchronized {
      configRef.set(newConfig)
      sequenceData.headOption.foreach { case (nextMinimumTimestamp, nextData) =>
        pushDownstreamIfConsensusIsReached(
          nextMinimumTimestamp,
          nextData,
        )
      }
    }
  }

  @SuppressWarnings(Array("NonUnitForEach"))
  override protected def onClosed(): Unit =
    blocking {
      this.synchronized {
        sequenceData.view.values
          .foreach(_.promise.shutdown_())
      }
    }
}
object SequencerAggregator {
  final case class MessageAggregationConfig(
      expectedSequencersO: Option[NonEmpty[Set[SequencerId]]],
      sequencerTrustThreshold: PositiveInt,
  )
  sealed trait SequencerAggregatorError extends Product with Serializable with PrettyPrinting
  object SequencerAggregatorError {
    final case class NotTheSameContentHash(hashes: NonEmpty[Set[Hash]])
        extends SequencerAggregatorError {
      override protected def pretty: Pretty[NotTheSameContentHash] =
        prettyOfClass(param("hashes", _.hashes))
    }
  }

  def aggregateHealthResult(
      healthResult: Map[SequencerId, ComponentHealthState],
      threshold: PositiveInt,
      associatedHasRunOnClosing: HasRunOnClosing,
  ): ComponentHealthState =
    NonEmpty.from(healthResult) match {
      case None => ComponentHealthState.NotInitializedState
      case Some(healthResultNE) if healthResult.sizeIs == 1 && threshold == PositiveInt.one =>
        // If only one sequencer ID is configured and threshold is one, forward the sequencer's health state unchanged
        // for backwards compatibility
        val (_, state) = healthResultNE.head1
        state
      case Some(_) =>
        // Healthy if at least `threshold` many sequencer connections are healthy, else
        // Degraded if at least `threshold` many sequencer connections are healthy or degraded, else
        // Failed

        val iter = healthResult.iterator

        @tailrec
        def go(
            healthyCount: Int,
            failed: Seq[SequencerId],
            degraded: Seq[SequencerId],
        ): ComponentHealthState =
          if (healthyCount >= threshold.value) ComponentHealthState.Ok()
          else if (!iter.hasNext) {
            val common =
              s"The sequencer client's healthy subscriptions count is under the configured BFT threshold (${threshold.value})."
            val failureMsg = Option.when(failed.nonEmpty)(
              s"Failed sequencer subscriptions for [${failed.sortBy(_.toProtoPrimitive).mkString(", ")}]."
            )
            val degradationMsg = Option.when(degraded.nonEmpty)(
              s"Degraded sequencer subscriptions for [${degraded.sortBy(_.toProtoPrimitive).mkString(", ")}]."
            )
            val message = Seq(Some(common), failureMsg, degradationMsg).flatten.mkString(" ")
            if (degraded.sizeIs >= threshold.value - healthyCount)
              ComponentHealthState.degraded(message)
            else
              ComponentHealthState.failed(
                message,
                // Don't log at WARN level if the sequencer client is closing
                logLevel = if (associatedHasRunOnClosing.isClosing) Level.INFO else Level.WARN,
              )
          } else {
            val (sequencerId, state) = iter.next()
            if (state.isOk) go(healthyCount + 1, failed, degraded)
            else if (state.isFailed) go(healthyCount, sequencerId +: failed, degraded)
            else go(healthyCount, failed, sequencerId +: degraded)
          }

        go(0, Seq.empty, Seq.empty)
    }
}
