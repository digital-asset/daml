// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.data.Chain
import cats.syntax.either.*
import com.digitalasset.canton.checked
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.InFlightAggregation.AggregationBySender
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{ErrorLoggingContext, HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.sequencing.protocol.{AggregationId, AggregationRule}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil.*
import com.google.common.annotations.VisibleForTesting

import scala.collection.immutable.SortedMap

/** Stores the state of an in-flight aggregation of submission requests.
  *
  * Since the [[com.digitalasset.canton.sequencing.protocol.AggregationId]] computationally
  * identifies the envelope contents, their recipients, and the
  * [[com.digitalasset.canton.sequencing.protocol.SubmissionRequest.topologyTimestamp]],
  * we do not need to maintain these data as part of the in-flight tracking.
  * Instead, we can derive them from the submission request that makes the aggregation reach its threshold.
  *
  * @param aggregatedSenders The senders whose submission request have already been aggregated
  *                          with the timestamp of their aggregated submission request and the signatures on the envelopes.
  * @param maxSequencingTimestamp The max sequencing timestamp of the aggregatable submission requests.
  *                               The aggregation will stop being in-flight when this timestamp has elapsed
  * @param rule The aggregation rule describing the eligible members and the threshold to reach
  */
final case class InFlightAggregation private (
    aggregatedSenders: SortedMap[Member, AggregationBySender],
    maxSequencingTimestamp: CantonTimestamp,
    rule: AggregationRule,
) extends PrettyPrinting
    with HasLoggerName {
  import InFlightAggregation.*

  /** The sequencing timestamp at which this aggregatable submission was delivered, if so. */
  lazy val deliveredAt: Option[CantonTimestamp] =
    Option
      .when(aggregatedSenders.sizeCompare(rule.threshold.value) >= 0)(
        aggregatedSenders.values.map(_.sequencingTimestamp).maxOption
      )
      .flatten

  /** The aggregated signatures on the closed envelopes in the aggregatable submission request,
    * in the same order as the envelopes are in the batch.
    *
    * The signatures for each envelope are ordered by the sender who produced them,
    * rather than the order in which the senders' submission requests were sequenced.
    * This avoids leaking the order of internal sequencing, as the signatures themselves anyway leak the sender
    * through the signing key's fingerprint.
    */
  def aggregatedSignatures: Seq[Seq[Signature]] =
    aggregatedSenders.values.map(_.signatures).transpose.map(_.flatten.toSeq).toSeq

  def tryAggregate(
      aggregatedSender: AggregatedSender
  ): Either[InFlightAggregationError, InFlightAggregation] = {
    val sender = aggregatedSender.sender
    val timestamp = aggregatedSender.aggregation.sequencingTimestamp
    require(
      timestamp <= maxSequencingTimestamp,
      s"Cannot aggregate submission by $sender with sequencing timestamp $timestamp after the max sequencing time at $maxSequencingTimestamp",
    )
    for {
      _ <- deliveredAt.toLeft(()).leftMap(AlreadyDelivered)
      _ <- aggregatedSenders
        .get(sender)
        .toLeft(())
        .leftMap(aggregationBySender =>
          AggregationStuffing(sender, aggregationBySender.sequencingTimestamp)
        )
    } yield this.copy(
      aggregatedSenders = aggregatedSenders + (sender -> aggregatedSender.aggregation)
    )
  }

  def asUpdate: InFlightAggregationUpdate = InFlightAggregationUpdate(
    Some(FreshInFlightAggregation(maxSequencingTimestamp, rule)),
    Chain(
      aggregatedSenders
        .map { case (sender, aggregationBySender) =>
          AggregatedSender(sender, aggregationBySender)
        }
        .to(Seq)*
    ),
  )

  /** Returns whether the in-flight aggregation has expired before or at the given timestamp.
    * An expired in-flight aggregation is no longer needed and can be removed.
    */
  def expired(timestamp: CantonTimestamp): Boolean = timestamp >= maxSequencingTimestamp

  /** Undoes all changes to the in-flight aggregation state that happened after the given timestamp.
    *
    * @return [[scala.None$]] if the aggregation is not in-flight at the given timestamp.
    *         An aggregation in in-flight from the first [[aggregatedSenders]]' timestamp to the [[maxSequencingTimestamp]].
    */
  def project(timestamp: CantonTimestamp): Option[InFlightAggregation] =
    for {
      _ <- Option.when(maxSequencingTimestamp > timestamp)(())
      projectedSenders = aggregatedSenders.filter {
        case (_sender, AggregationBySender(sequencingTimestamp, _signatures)) =>
          sequencingTimestamp <= timestamp
      }
      _ <- Option.when(projectedSenders.nonEmpty)(())
    } yield new InFlightAggregation(projectedSenders, maxSequencingTimestamp, rule)

  override def pretty: Pretty[this.type] = prettyOfClass(
    param("aggregated senders", _.aggregatedSenders),
    param("max sequencing time", _.maxSequencingTimestamp),
    paramIfNonEmpty("sequencing timestamp", _.deliveredAt),
    param("rule", _.rule),
  )

  @VisibleForTesting
  def copy(
      aggregatedSenders: SortedMap[Member, AggregationBySender] = this.aggregatedSenders,
      maxSequencingTimestamp: CantonTimestamp = this.maxSequencingTimestamp,
      rule: AggregationRule = this.rule,
  ): InFlightAggregation =
    InFlightAggregation.tryCreate(aggregatedSenders, maxSequencingTimestamp, rule)

  /** @throws java.lang.IllegalStateException if the class invariant does not hold */
  def checkInvariant()(implicit loggingContext: NamedLoggingContext): Unit = {
    InFlightAggregation
      .checkInvariant(aggregatedSenders, maxSequencingTimestamp, rule)
      .valueOr(err => ErrorUtil.invalidState(err))
  }
}

object InFlightAggregation {
  def create(
      aggregatedSenders: Map[Member, AggregationBySender],
      maxSequencingTimestamp: CantonTimestamp,
      rule: AggregationRule,
  ): Either[String, InFlightAggregation] =
    checkInvariant(aggregatedSenders, maxSequencingTimestamp, rule).map(_ =>
      new InFlightAggregation(
        SortedMap.from(aggregatedSenders),
        maxSequencingTimestamp,
        rule,
      )
    )

  def tryCreate(
      aggregatedSenders: Map[Member, AggregationBySender],
      maxSequencingTimestamp: CantonTimestamp,
      rule: AggregationRule,
  ): InFlightAggregation =
    create(aggregatedSenders, maxSequencingTimestamp, rule)
      .valueOr(err => throw new IllegalArgumentException(err))

  @VisibleForTesting
  def apply(
      rule: AggregationRule,
      maxSequencingTimestamp: CantonTimestamp,
      aggregatedSenders: (Member, AggregationBySender)*
  ): InFlightAggregation =
    InFlightAggregation.tryCreate(
      aggregatedSenders = Map.from(aggregatedSenders),
      maxSequencingTimestamp,
      rule,
    )

  def initial(fresh: FreshInFlightAggregation): InFlightAggregation =
    checked(tryCreate(Map.empty, fresh.maxSequencingTimestamp, fresh.rule))

  private def checkInvariant(
      aggregatedSenders: Map[Member, AggregationBySender],
      maxSequencingTimestamp: CantonTimestamp,
      rule: AggregationRule,
  ): Either[String, Unit] = {
    val uneligibleAggregated = aggregatedSenders.keys.filterNot(rule.eligibleSenders.contains)
    for {
      _ <- Either.cond(
        uneligibleAggregated.isEmpty,
        (),
        show"non-eligible members' submission requests have been aggregated: ${uneligibleAggregated.toSeq}",
      )
      envelopeCounts = aggregatedSenders.values.map(_.signatures.size).toSet
      _ <- Either.cond(
        envelopeCounts.sizeIs <= 1,
        (),
        show"aggregated senders have varying numbers of envelopes: $envelopeCounts",
      )
      lateSenders = aggregatedSenders.collect {
        case (sender, aggregationBySender)
            if aggregationBySender.sequencingTimestamp > maxSequencingTimestamp =>
          sender -> aggregationBySender.sequencingTimestamp
      }
      _ <- Either.cond(
        lateSenders.isEmpty,
        (),
        show"aggregated senders' sequencing timestamp is after the max sequencing time at $maxSequencingTimestamp: $aggregatedSenders",
      )
    } yield ()
  }

  sealed trait InFlightAggregationError extends Product with Serializable

  /** The aggregatable submission was already delivered at the given timestamp. */
  final case class AlreadyDelivered(deliveredAt: CantonTimestamp) extends InFlightAggregationError

  /** The given sender has already contributed its aggregatable submission request, which was sequenced at the given timestamp */
  final case class AggregationStuffing(sender: Member, sequencingTimestamp: CantonTimestamp)
      extends InFlightAggregationError

  final case class AggregationBySender(
      sequencingTimestamp: CantonTimestamp,
      signatures: Seq[Seq[Signature]],
  ) extends PrettyPrinting {
    override def pretty: Pretty[this.type] = prettyOfClass(
      param("sequencing timestamp", _.sequencingTimestamp),
      param("signatures", _.signatures),
    )
  }

  def tryApplyUpdate(
      aggregationId: AggregationId,
      inFlightAggregationO: Option[InFlightAggregation],
      update: InFlightAggregationUpdate,
      ignoreInFlightAggregationErrors: Boolean,
  )(implicit loggingContext: ErrorLoggingContext): InFlightAggregation = {
    val InFlightAggregationUpdate(freshO, aggregatedSenders) = update
    val inFlightAggregation = inFlightAggregationO match {
      case None =>
        val fresh = freshO.getOrElse(
          ErrorUtil.internalError(
            new IllegalArgumentException(
              s"Missing in-flight aggregation information for ID $aggregationId"
            )
          )
        )
        InFlightAggregation.initial(fresh)
      case Some(inFlightAggregation) =>
        freshO.foreach { fresh =>
          val existing = FreshInFlightAggregation(
            inFlightAggregation.maxSequencingTimestamp,
            inFlightAggregation.rule,
          )
          ErrorUtil.requireArgument(
            fresh == existing,
            s"Mismatch with existing in-flight aggregation: existing: $existing, new: $fresh",
          )
        }
        inFlightAggregation
    }
    aggregatedSenders.foldLeft(inFlightAggregation)((inFlightAgg, senderAggregation) =>
      inFlightAgg
        .tryAggregate(senderAggregation)
        .valueOr(err =>
          if (ignoreInFlightAggregationErrors) inFlightAgg
          else
            ErrorUtil.internalError(
              new IllegalArgumentException(
                s"Failed to apply aggregation update for id $aggregationId: $err"
              )
            )
        )
    )
  }
}
