// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{CryptoPureApi, Hash, HashPurpose, Signature}
import com.digitalasset.canton.data.CantonTimestamp
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
    val timestampsOfSigningKey = messages.map(_.signedEvent.timestampOfSigningKey).toSet
    for {
      _ <- Either.cond(
        hashes.forall(_ == expectedMessageHash),
        (),
        SequencerAggregatorError.NotTheSameContentHash(hashes),
      )
      expectedTimestampOfSigningKey = message.signedEvent.timestampOfSigningKey
      _ <- Either.cond(
        messages.forall(_.signedEvent.timestampOfSigningKey == expectedTimestampOfSigningKey),
        (),
        SequencerAggregatorError.NotTheSameTimestampOfSigningKey(timestampsOfSigningKey),
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
    final case class NotTheSameTimestampOfSigningKey(
        timestamps: NonEmpty[Set[Option[CantonTimestamp]]]
    ) extends SequencerAggregatorError {
      override def pretty: Pretty[NotTheSameTimestampOfSigningKey] =
        prettyOfClass(param("timestamps", _.timestamps))
    }
  }
}
