// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.traffic

import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, NonNegativeNumeric}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30.TrafficConsumed as TrafficConsumedP
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member
import slick.jdbc.GetResult

/** State of the traffic consumed by a member at a given time.
  * @param member Member consuming the traffic
  * @param sequencingTimestamp sequencing timestamp at which this traffic consumed state is valid
  * @param extraTrafficConsumed extra traffic consumed at this sequencing timestamp
  * @param baseTrafficRemainder base traffic remaining at this sequencing timestamp
  */
final case class TrafficConsumed(
    member: Member,
    sequencingTimestamp: CantonTimestamp,
    extraTrafficConsumed: NonNegativeLong,
    baseTrafficRemainder: NonNegativeLong,
) extends PrettyPrinting {
  private def computeBaseTrafficAt(
      timestamp: CantonTimestamp,
      trafficControlConfig: TrafficControlParameters,
  ) = {
    val deltaMicros = timestamp.toMicros - this.sequencingTimestamp.toMicros
    val trafficAllowedSinceLastTimestamp = Math
      .floor(trafficControlConfig.baseRate.value * deltaMicros.toDouble / 1e6)
      .toLong

    NonNegativeLong
      .create(
        Math.min(
          trafficControlConfig.maxBaseTrafficAmount.value,
          trafficAllowedSinceLastTimestamp + baseTrafficRemainder.value,
        )
      )
      .leftMap { e =>
        s"Failed to compute base traffic at $timestamp for member $member." +
          s"The provided timestamp for the computation ($timestamp) is before the timestamp of the know consumed traffic ($sequencingTimestamp)" +
          s", which results in a negative delta. Please report this as a bug."
      }
  }

  def consume(
      sequencingTimestamp: CantonTimestamp,
      trafficControlConfig: TrafficControlParameters,
      cost: NonNegativeLong,
  ): Either[String, TrafficConsumed] = {
    // determine the time elapsed since last update
    computeBaseTrafficAt(sequencingTimestamp, trafficControlConfig).map {
      baseTrafficRemainderAtCurrentTime =>
        val NonNegativeNumeric.SubtractionResult(
          baseTrafficRemainderAfterConsume,
          extraTrafficConsumed,
        ) =
          baseTrafficRemainderAtCurrentTime.subtract(cost)

        copy(
          baseTrafficRemainder = baseTrafficRemainderAfterConsume,
          extraTrafficConsumed = this.extraTrafficConsumed + extraTrafficConsumed,
          sequencingTimestamp = sequencingTimestamp,
        )
    }
  }

  override def pretty: Pretty[TrafficConsumed] =
    prettyOfClass(
      param("member", _.member),
      param("extraTrafficConsumed", _.extraTrafficConsumed),
      param("baseTrafficRemainder", _.baseTrafficRemainder),
      param("sequencingTimestamp", _.sequencingTimestamp),
    )

  def toProtoV30: TrafficConsumedP = {
    TrafficConsumedP(
      member = member.toProtoPrimitive,
      extraTrafficConsumed = extraTrafficConsumed.value,
      baseTrafficRemainder = baseTrafficRemainder.value,
      sequencingTimestamp = sequencingTimestamp.toProtoPrimitive,
    )
  }
}

object TrafficConsumed {

  import com.digitalasset.canton.store.db.RequiredTypesCodec.*
  import com.digitalasset.canton.topology.Member.DbStorageImplicits.*

  /** TrafficConsumed object for members the first time they submit a submission request
    */
  def init(member: Member): TrafficConsumed =
    TrafficConsumed(member, CantonTimestamp.MinValue, NonNegativeLong.zero, NonNegativeLong.zero)

  implicit val trafficConsumedOrdering: Ordering[TrafficConsumed] =
    Ordering.by(_.sequencingTimestamp)

  implicit val trafficConsumedGetResult: GetResult[TrafficConsumed] =
    GetResult.createGetTuple4[Member, CantonTimestamp, NonNegativeLong, NonNegativeLong].andThen {
      case (member, ts, trafficConsumed, baseTraffic) =>
        TrafficConsumed(member, ts, trafficConsumed, baseTraffic)
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
    } yield TrafficConsumed(
      member = member,
      extraTrafficConsumed = extraTrafficConsumed,
      baseTrafficRemainder = baseTrafficRemainder,
      sequencingTimestamp = sequencingTimestamp,
    )
}
