// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{CryptoPureApi, Hash, HashPurpose, Signature}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.ComponentHealthState
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.SequencerAggregator.{
  MessageAggregationConfig,
  SequencerAggregatorError,
}
import com.digitalasset.canton.sequencing.protocol.SignedContent
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.google.common.annotations.VisibleForTesting

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
    override val timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
) extends NamedLogging
    with FlagCloseable {

  private val configRef: AtomicReference[MessageAggregationConfig] =
    new AtomicReference[MessageAggregationConfig](initialConfig)
  def expectedSequencers: NonEmpty[Set[SequencerId]] = configRef.get().expectedSequencers

  def sequencerTrustThreshold: PositiveInt = configRef.get().sequencerTrustThreshold

  private case class SequencerMessageData(
      eventBySequencer: Map[SequencerId, OrdinarySerializedEvent],
      promise: PromiseUnlessShutdown[Either[SequencerAggregatorError, SequencerId]],
  )

  /** Queue containing received and not yet handled events.
    * Used for batched processing.
    */
  private val receivedEvents: BlockingQueue[OrdinarySerializedEvent] =
    new ArrayBlockingQueue[OrdinarySerializedEvent](eventInboxSize.unwrap)

  private val sequenceData = mutable.TreeMap.empty[CantonTimestamp, SequencerMessageData]

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var cursor: Option[CantonTimestamp] = None

  def eventQueue: BlockingQueue[OrdinarySerializedEvent] = receivedEvents

  private def hash(message: OrdinarySerializedEvent) =
    SignedContent.hashContent(
      cryptoPureApi,
      message.signedEvent.content,
      HashPurpose.SequencedEventSignature,
    )

  @VisibleForTesting
  def combine(
      messages: NonEmpty[Seq[OrdinarySerializedEvent]]
  ): Either[SequencerAggregatorError, OrdinarySerializedEvent] = {
    val message: OrdinarySerializedEvent = messages.head1
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

      message.copy(signedEvent = message.signedEvent.copy(signatures = combinedSignatures))(
        potentiallyNonEmptyTraceContext
      )
    }
  }

  private def addEventToQueue(event: OrdinarySerializedEvent): Unit = {
    implicit val traceContext: TraceContext = event.traceContext
    logger.debug(
      show"Storing event in the event inbox.\n${event.signedEvent.content}"
    )
    if (!receivedEvents.offer(event)) {
      logger.debug(
        s"Event inbox is full. Blocking sequenced event with timestamp ${event.timestamp}."
      )
      blocking {
        receivedEvents.put(event)
      }
      logger.debug(
        s"Unblocked sequenced event with timestamp ${event.timestamp}."
      )
    }
  }

  private def addEventToQueue(
      messages: NonEmpty[List[OrdinarySerializedEvent]]
  ): Either[SequencerAggregatorError, Unit] =
    combine(messages).map(addEventToQueue)

  def combineAndMergeEvent(
      sequencerId: SequencerId,
      message: OrdinarySerializedEvent,
  )(implicit
      ec: ExecutionContext
  ): FutureUnlessShutdown[Either[SequencerAggregatorError, Boolean]] = {
    if (!expectedSequencers.contains(sequencerId)) {
      throw new IllegalArgumentException(s"Unexpected sequencerId: $sequencerId")
    }
    blocking {
      this.synchronized {
        if (cursor.forall(message.timestamp > _)) {
          val sequencerMessageData = updatedSequencerMessageData(sequencerId, message)
          sequenceData.put(message.timestamp, sequencerMessageData): Unit

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
  }

  private def pushDownstreamIfConsensusIsReached(
      nextMinimumTimestamp: CantonTimestamp,
      nextData: SequencerMessageData,
  ): Unit = {
    val expectedMessages = nextData.eventBySequencer.view.filterKeys { sequencerId =>
      expectedSequencers.contains(sequencerId)
    }

    if (expectedMessages.sizeCompare(sequencerTrustThreshold.unwrap) >= 0) {
      cursor = Some(nextMinimumTimestamp)
      sequenceData.remove(nextMinimumTimestamp): Unit

      val nonEmptyMessages = NonEmptyUtil.fromUnsafe(expectedMessages.toMap)
      val messagesToCombine = nonEmptyMessages.map(_._2).toList
      val (sequencerIdToNotify, _) = nonEmptyMessages.head1

      nextData.promise
        .outcome(
          addEventToQueue(messagesToCombine).map(_ => sequencerIdToNotify)
        )
    }
  }

  private def updatedSequencerMessageData(
      sequencerId: SequencerId,
      message: OrdinarySerializedEvent,
  )(implicit
      ec: ExecutionContext
  ): SequencerMessageData = {
    implicit val traceContext = message.traceContext
    val promise = new PromiseUnlessShutdown[Either[SequencerAggregatorError, SequencerId]](
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
          .foreach(_.promise.shutdown())
      }
    }
}
object SequencerAggregator {
  final case class MessageAggregationConfig(
      expectedSequencers: NonEmpty[Set[SequencerId]],
      sequencerTrustThreshold: PositiveInt,
  )
  sealed trait SequencerAggregatorError extends Product with Serializable with PrettyPrinting
  object SequencerAggregatorError {
    final case class NotTheSameContentHash(hashes: NonEmpty[Set[Hash]])
        extends SequencerAggregatorError {
      override def pretty: Pretty[NotTheSameContentHash] =
        prettyOfClass(param("hashes", _.hashes))
    }
  }

  def aggregateHealthResult(
      healthResult: Map[SequencerId, ComponentHealthState],
      threshold: PositiveInt,
  ): ComponentHealthState = {
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
        ): ComponentHealthState = {
          if (healthyCount >= threshold.value) ComponentHealthState.Ok()
          else if (!iter.hasNext) {
            val failureMsg = Option.when(failed.nonEmpty)(
              s"Failed sequencer subscriptions for [${failed.sortBy(_.toProtoPrimitive).mkString(", ")}]."
            )
            val degradationMsg = Option.when(degraded.nonEmpty)(
              s"Degraded sequencer subscriptions for [${degraded.sortBy(_.toProtoPrimitive).mkString(", ")}]."
            )
            val message = Seq(failureMsg, degradationMsg).flatten.mkString(" ")
            if (degraded.sizeIs >= threshold.value - healthyCount)
              ComponentHealthState.degraded(message)
            else ComponentHealthState.failed(message)
          } else {
            val (sequencerId, state) = iter.next()
            if (state.isOk) go(healthyCount + 1, failed, degraded)
            else if (state.isFailed) go(healthyCount, sequencerId +: failed, degraded)
            else go(healthyCount, failed, sequencerId +: degraded)
          }
        }

        go(0, Seq.empty, Seq.empty)
    }
  }
}
