// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.traffic

import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, NonNegativeNumeric}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.protocol.v30.TrafficConsumed as TrafficConsumedP
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.sequencing.traffic.TrafficConsumedManager.NotEnoughTraffic
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import slick.jdbc.GetResult

/** State of the traffic consumed by a member at a given time.
  * @param member Member consuming the traffic
  * @param sequencingTimestamp sequencing timestamp at which this traffic consumed state is valid
  * @param extraTrafficConsumed extra traffic consumed at this sequencing timestamp
  * @param baseTrafficRemainder base traffic remaining at this sequencing timestamp
  * @param lastConsumedCost last cost deducted from the traffic balance (base and if not enough, extra)
  */
final case class TrafficConsumed(
    member: Member,
    sequencingTimestamp: CantonTimestamp,
    extraTrafficConsumed: NonNegativeLong,
    baseTrafficRemainder: NonNegativeLong,
    lastConsumedCost: NonNegativeLong,
) extends PrettyPrinting {

  def toTrafficReceipt: TrafficReceipt = TrafficReceipt(
    consumedCost = lastConsumedCost,
    extraTrafficConsumed,
    baseTrafficRemainder,
  )

  /** Compute the traffic state off of this traffic consumed and the provided optional traffic purchased.
    * The caller MUST guarantee that the TrafficPurchased is correct at this.sequencingTime.
    */
  def toTrafficState(trafficPurchased: Option[TrafficPurchased]): TrafficState = {
    TrafficState(
      trafficPurchased.map(_.extraTrafficPurchased).getOrElse(NonNegativeLong.zero),
      extraTrafficConsumed,
      baseTrafficRemainder,
      lastConsumedCost,
      trafficPurchased
        .map(_.sequencingTimestamp.max(sequencingTimestamp))
        .getOrElse(sequencingTimestamp),
      trafficPurchased.map(_.serial),
    )
  }

  /** Compute the base traffic at a given timestamp according to the provided parameters.
    * Generally this method should be called with a timestamp that is more recent than sequencingTimestamp.
    * However when calculating the available traffic at submission time, we use the timestamp of the last known
    * sequenced event as the most recent domain time we know about. It is possible though that by the time
    * we reach this method, the traffic consumed was updated concurrently by a new block being read and
    * which consumed traffic for this member. We use the parameter allowOldTimestamp in such cases
    * to avoid throwing an error.
    */
  private def computeBaseTrafficAt(
      timestamp: CantonTimestamp,
      trafficControlConfig: TrafficControlParameters,
      logger: TracedLogger,
  )(implicit traceContext: TraceContext): NonNegativeLong = {
    implicit val elc: ErrorLoggingContext = ErrorLoggingContext.fromTracedLogger(logger)
    val deltaMicros = NonNegativeNumeric
      .create(timestamp.toMicros.toDouble - this.sequencingTimestamp.toMicros.toDouble)
      .valueOr { _ =>
        ErrorUtil.invalidState(
          s"Failed to compute base traffic at $timestamp for member $member." +
            s"The provided timestamp for the computation ($timestamp) is before the timestamp of the known consumed traffic ($sequencingTimestamp)" +
            s", which results in a negative delta. Please report this as a bug."
        )
      }

    val trafficAllowedSinceLastTimestamp: NonNegativeLong = (
      trafficControlConfig.baseRate.map(_.toDouble) * deltaMicros / NonNegativeNumeric
        .tryCreate[Double](1e6d)
    ).map(_.toLong)

    implicitly[Ordering[NonNegativeLong]].min(
      trafficControlConfig.maxBaseTrafficAmount,
      trafficAllowedSinceLastTimestamp + baseTrafficRemainder,
    )
  }

  /** Update the base traffic remainder to timestamp.
    * This should ONLY be called with timestamp > this.sequencingTimestamp.
    */
  def updateTimestamp(
      timestamp: CantonTimestamp,
      trafficControlConfig: TrafficControlParameters,
      tracedLogger: TracedLogger,
  )(implicit traceContext: TraceContext): TrafficConsumed = {
    // Compute the base traffic available at sequencing time
    val baseTrafficRemainderAtCurrentTime =
      computeBaseTrafficAt(timestamp, trafficControlConfig, tracedLogger)

    copy(
      baseTrafficRemainder = baseTrafficRemainderAtCurrentTime,
      sequencingTimestamp = timestamp,
      lastConsumedCost = NonNegativeLong.zero,
    )
  }

  def consume(
      sequencingTimestamp: CantonTimestamp,
      trafficControlConfig: TrafficControlParameters,
      cost: NonNegativeLong,
      tracedLogger: TracedLogger,
  )(implicit traceContext: TraceContext): TrafficConsumed = {
    // Compute the base traffic available at sequencing time
    val baseTrafficRemainderAtCurrentTime =
      computeBaseTrafficAt(sequencingTimestamp, trafficControlConfig, tracedLogger)
    val NonNegativeNumeric.SubtractionResult(
      baseTrafficRemainderAfterConsume,
      extraTrafficConsumed,
    ) =
      baseTrafficRemainderAtCurrentTime.subtract(cost)

    copy(
      baseTrafficRemainder = baseTrafficRemainderAfterConsume,
      extraTrafficConsumed = this.extraTrafficConsumed + extraTrafficConsumed,
      sequencingTimestamp = sequencingTimestamp,
      lastConsumedCost = cost,
    )
  }

  def canConsumeAt(
      params: TrafficControlParameters,
      cost: NonNegativeLong,
      timestamp: CantonTimestamp,
      trafficBalanceO: Option[TrafficPurchased],
      logger: TracedLogger,
  )(implicit traceContext: TraceContext): Either[NotEnoughTraffic, Unit] = {
    val baseTrafficRemainderAtCurrentTime =
      computeBaseTrafficAt(timestamp, params, logger)

    val updatedTrafficState = toTrafficState(trafficBalanceO)
      .copy(
        baseTrafficRemainder = baseTrafficRemainderAtCurrentTime,
        timestamp = timestamp,
      )
    Either.cond(
      updatedTrafficState.availableTraffic >= cost.value,
      (),
      NotEnoughTraffic(member, cost, updatedTrafficState),
    )
  }

  override def pretty: Pretty[TrafficConsumed] =
    prettyOfClass(
      param("member", _.member),
      param("extraTrafficConsumed", _.extraTrafficConsumed),
      param("baseTrafficRemainder", _.baseTrafficRemainder),
      param("lastConsumedCost", _.lastConsumedCost),
      param("sequencingTimestamp", _.sequencingTimestamp),
    )

  def toProtoV30: TrafficConsumedP = {
    TrafficConsumedP(
      member = member.toProtoPrimitive,
      extraTrafficConsumed = extraTrafficConsumed.value,
      baseTrafficRemainder = baseTrafficRemainder.value,
      sequencingTimestamp = sequencingTimestamp.toProtoPrimitive,
      lastConsumedCost = lastConsumedCost.value,
    )
  }
}

object TrafficConsumed {
  import com.digitalasset.canton.store.db.RequiredTypesCodec.*
  import com.digitalasset.canton.topology.Member.DbStorageImplicits.*

  /** TrafficConsumed object for members the first time they submit a submission request
    */
  def init(member: Member): TrafficConsumed =
    TrafficConsumed(
      member,
      CantonTimestamp.MinValue,
      NonNegativeLong.zero,
      NonNegativeLong.zero,
      NonNegativeLong.zero,
    )

  def empty(
      member: Member,
      timestamp: CantonTimestamp,
      baseTraffic: NonNegativeLong,
  ): TrafficConsumed = TrafficConsumed(
    member,
    timestamp,
    NonNegativeLong.zero,
    baseTraffic,
    NonNegativeLong.zero,
  )

  implicit val trafficConsumedOrdering: Ordering[TrafficConsumed] =
    Ordering.by(_.sequencingTimestamp)

  implicit val trafficConsumedGetResult: GetResult[TrafficConsumed] =
    GetResult
      .createGetTuple5[Member, CantonTimestamp, NonNegativeLong, NonNegativeLong, NonNegativeLong]
      .andThen { case (member, ts, trafficConsumed, baseTraffic, lastConsumedCost) =>
        TrafficConsumed(member, ts, trafficConsumed, baseTraffic, lastConsumedCost)
      }

  def fromProtoV30(trafficConsumedP: TrafficConsumedP): ParsingResult[TrafficConsumed] =
    for {
      member <- Member.fromProtoPrimitive(trafficConsumedP.member, "member")
      extraTrafficConsumed <- ProtoConverter.parseNonNegativeLong(
        trafficConsumedP.extraTrafficConsumed
      )
      baseTrafficRemainder <- ProtoConverter.parseNonNegativeLong(
        trafficConsumedP.baseTrafficRemainder
      )
      sequencingTimestamp <- CantonTimestamp.fromProtoPrimitive(
        trafficConsumedP.sequencingTimestamp
      )
      lastConsumedCost <- ProtoConverter.parseNonNegativeLong(
        trafficConsumedP.lastConsumedCost
      )
    } yield TrafficConsumed(
      member = member,
      extraTrafficConsumed = extraTrafficConsumed,
      baseTrafficRemainder = baseTrafficRemainder,
      sequencingTimestamp = sequencingTimestamp,
      lastConsumedCost = lastConsumedCost,
    )
}
