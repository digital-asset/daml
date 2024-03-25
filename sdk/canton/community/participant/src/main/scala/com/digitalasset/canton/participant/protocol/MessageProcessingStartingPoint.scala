// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.syntax.either.*
import com.digitalasset.canton.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.LocalOffset
import com.digitalasset.canton.participant.protocol.ProcessingStartingPoints.InvalidStartingPointsException
import com.digitalasset.canton.store.CursorPrehead.SequencerCounterCursorPrehead
import com.google.common.annotations.VisibleForTesting

/** Summarizes the counters and timestamps where request processing
  *
  * @param cleanRequestPrehead The request offset corresponding to the prehead of the clean request if any.
  * @param nextRequestCounter The request counter for the next request to be replayed or processed.
  * @param nextSequencerCounter The sequencer counter for the next event to be replayed or processed.
  * @param prenextTimestamp A strict lower bound on the timestamp for the `nextSequencerCounter`.
  *                         The bound must be tight, i.e., if a sequenced event has sequencer counter lower than
  *                         `nextSequencerCounter` or request counter lower than `nextRequestCounter`,
  *                         then the timestamp of the event must be less than or equal to `prenextTimestamp`.
  *
  *                         No sequenced event has both a higher timestamp than `prenextTimestamp`
  *                         and a lower sequencer counter than `nextSequencerCounter`.
  *                         No request has both a higher timestamp than `prenextTimestamp`
  *                         and a lower request counter than `nextRequestCounter`.
  */
final case class MessageProcessingStartingPoint(
    cleanRequestPrehead: Option[RequestCounter],
    nextRequestCounter: RequestCounter,
    nextSequencerCounter: SequencerCounter,
    prenextTimestamp: CantonTimestamp,
) extends PrettyPrinting {
  override def pretty: Pretty[MessageProcessingStartingPoint] = prettyOfClass(
    paramIfDefined("last clean offset", _.cleanRequestPrehead),
    param("next request counter", _.nextRequestCounter),
    param("next sequencer counter", _.nextSequencerCounter),
    param("prenext timestamp", _.prenextTimestamp),
  )

  def toMessageCleanReplayStartingPoint: MessageCleanReplayStartingPoint =
    MessageCleanReplayStartingPoint(nextRequestCounter, nextSequencerCounter, prenextTimestamp)
}

object MessageProcessingStartingPoint {
  def default: MessageProcessingStartingPoint =
    MessageProcessingStartingPoint(
      None,
      RequestCounter.Genesis,
      SequencerCounter.Genesis,
      CantonTimestamp.MinValue,
    )
}

/** Summarizes the counters and timestamps where replay can start
  *
  * @param nextRequestCounter The request counter for the next request to be replayed
  * @param nextSequencerCounter The sequencer counter for the next event to be replayed
  * @param prenextTimestamp A strict lower bound on the timestamp for the `nextSequencerCounter`.
  *                         The bound must be tight, i.e., if a sequenced event has sequencer counter lower than
  *                         `nextSequencerCounter` or request counter lower than `nextRequestCounter`,
  *                         then the timestamp of the event must be less than or equal to `prenextTimestamp`.
  *
  *                         No sequenced event has both a higher timestamp than `prenextTimestamp`
  *                         and a lower sequencer counter than `nextSequencerCounter`.
  *                         No request has both a higher timestamp than `prenextTimestamp`
  *                         and a lower request counter than `nextRequestCounter`.
  */
final case class MessageCleanReplayStartingPoint(
    nextRequestCounter: RequestCounter,
    nextSequencerCounter: SequencerCounter,
    prenextTimestamp: CantonTimestamp,
) extends PrettyPrinting {

  override def pretty: Pretty[MessageCleanReplayStartingPoint] = prettyOfClass(
    param("next request counter", _.nextRequestCounter),
    param("next sequencer counter", _.nextSequencerCounter),
    param("prenext timestamp", _.prenextTimestamp),
  )
}

object MessageCleanReplayStartingPoint {
  def default: MessageCleanReplayStartingPoint =
    MessageCleanReplayStartingPoint(
      RequestCounter.Genesis,
      SequencerCounter.Genesis,
      CantonTimestamp.MinValue,
    )
}

/** Starting points for processing on a [[com.digitalasset.canton.participant.sync.SyncDomain]].
  * The `cleanReplay` should be no later than the `processing` (in all components).
  *
  * @param cleanReplay The starting point for replaying clean requests
  * @param processing                     The starting point for processing requests.
  *                                       It refers to the first request that is not known to be clean.
  *                                       The [[MessageProcessingStartingPoint.prenextTimestamp]] be the timestamp of a sequenced event
  *                                       or [[com.digitalasset.canton.data.CantonTimestamp.MinValue]].
  * @param lastPublishedLocalOffset       The last local offset that was published to the
  *                                       [[com.digitalasset.canton.participant.store.MultiDomainEventLog]]
  * @param rewoundSequencerCounterPrehead The point to which the sequencer counter prehead needs to be reset as part of the recovery clean-up.
  *                                       This is the minimum of the following:
  *                                       * The last event before the [[processing]] event
  *                                       * The clean sequencer counter prehead at the beginning of crash recovery.
  * @throws ProcessingStartingPoints.InvalidStartingPointsException if `cleanReplay` is after (in any component) `processing`
  */
final case class ProcessingStartingPoints private (
    cleanReplay: MessageCleanReplayStartingPoint,
    processing: MessageProcessingStartingPoint,
    // TODO(#15089) Check whether this should be LocalOffset
    lastPublishedLocalOffset: Option[LocalOffset],
    rewoundSequencerCounterPrehead: Option[SequencerCounterCursorPrehead],
) extends PrettyPrinting {

  if (cleanReplay.prenextTimestamp > processing.prenextTimestamp)
    throw InvalidStartingPointsException(
      s"Clean replay pre-next timestamp ${cleanReplay.prenextTimestamp} is after processing pre-next timestamp ${processing.prenextTimestamp}"
    )
  if (cleanReplay.nextRequestCounter > processing.nextRequestCounter)
    throw InvalidStartingPointsException(
      s"Clean replay next request counter ${cleanReplay.nextRequestCounter} is after processing next request counter ${processing.nextRequestCounter}"
    )
  if (cleanReplay.nextSequencerCounter > processing.nextSequencerCounter)
    throw InvalidStartingPointsException(
      s"Clean replay next sequencer counter ${cleanReplay.nextSequencerCounter} is after processing next sequencer counter ${processing.nextSequencerCounter}"
    )

  /** No events should have been published at or after the point where processing starts again
    * because [[com.digitalasset.canton.participant.protocol.AbstractMessageProcessor.terminateRequest]]
    * ticks the [[com.digitalasset.canton.participant.event.RecordOrderPublisher]] only after
    * the clean request prehead has advanced in the [[RequestJournal]]. So this method normally returns `true`.
    *
    * Some tests reset the clean request prehead and thus violate this invariant.
    * In such a case, this method returns `false.`
    */
  def processingAfterPublished: Boolean =
    (lastPublishedLocalOffset, processing.cleanRequestPrehead) match {
      case (Some(lastPublishedLocalOffset), Some(processingCleanRequestPrehead)) =>
        processingCleanRequestPrehead >= lastPublishedLocalOffset.requestCounter
      case (Some(_lastPublishedLocalOffset), None) => false
      case (None, Some(_lastProcessed)) => true
      case (None, None) => true
    }

  override def pretty: Pretty[ProcessingStartingPoints] = prettyOfClass(
    param("clean replay", _.cleanReplay),
    param("processing", _.processing),
    paramIfDefined("last published request offset", _.lastPublishedLocalOffset),
    param("rewound sequencer counter prehead", _.rewoundSequencerCounterPrehead),
  )
}

object ProcessingStartingPoints {
  final case class InvalidStartingPointsException(message: String) extends RuntimeException(message)

  def tryCreate(
      cleanReplay: MessageCleanReplayStartingPoint,
      processing: MessageProcessingStartingPoint,
      lastPublishedLocalOffset: Option[LocalOffset],
      rewoundSequencerCounterPrehead: Option[SequencerCounterCursorPrehead],
  ): ProcessingStartingPoints =
    new ProcessingStartingPoints(
      cleanReplay,
      processing,
      lastPublishedLocalOffset,
      rewoundSequencerCounterPrehead,
    )

  def create(
      cleanReplay: MessageCleanReplayStartingPoint,
      processing: MessageProcessingStartingPoint,
      lastPublishedLocalOffset: Option[LocalOffset],
      rewoundSequencerCounterPrehead: Option[SequencerCounterCursorPrehead],
  ): Either[String, ProcessingStartingPoints] =
    Either
      .catchOnly[InvalidStartingPointsException](
        tryCreate(
          cleanReplay,
          processing,
          lastPublishedLocalOffset,
          rewoundSequencerCounterPrehead,
        )
      )
      .leftMap(_.message)

  @VisibleForTesting
  private[protocol] def default: ProcessingStartingPoints =
    new ProcessingStartingPoints(
      cleanReplay = MessageCleanReplayStartingPoint.default,
      processing = MessageProcessingStartingPoint.default,
      lastPublishedLocalOffset = None,
      rewoundSequencerCounterPrehead = None,
    )
}
