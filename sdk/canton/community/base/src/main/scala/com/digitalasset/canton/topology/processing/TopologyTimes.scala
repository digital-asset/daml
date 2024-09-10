// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.digitalasset.canton.LfTimestamp
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.RefinedDuration
import com.google.protobuf.timestamp.Timestamp as ProtoTimestamp

final case class EffectiveTime(value: CantonTimestamp) {
  def toApproximate: ApproximateTime = ApproximateTime(value)

  def toProtoPrimitive: ProtoTimestamp = value.toProtoTimestamp
  def toLf: LfTimestamp = value.toLf
  def max(that: EffectiveTime): EffectiveTime =
    EffectiveTime(value.max(that.value))
  def immediateSuccessor(): EffectiveTime = EffectiveTime(value.immediateSuccessor)
  def +(duration: RefinedDuration): EffectiveTime = EffectiveTime(value + duration)
}
object EffectiveTime {
  val MinValue: EffectiveTime = EffectiveTime(CantonTimestamp.MinValue)
  val MaxValue: EffectiveTime = EffectiveTime(CantonTimestamp.MaxValue)
  implicit val orderingEffectiveTime: Ordering[EffectiveTime] =
    Ordering.by[EffectiveTime, CantonTimestamp](_.value)
  def fromProtoPrimitive(ts: ProtoTimestamp): ParsingResult[EffectiveTime] =
    CantonTimestamp.fromProtoTimestamp(ts).map(EffectiveTime(_))
}

final case class ApproximateTime(value: CantonTimestamp)
object ApproximateTime {
  val MinValue: ApproximateTime = ApproximateTime(CantonTimestamp.MinValue)
  val MaxValue: ApproximateTime = ApproximateTime(CantonTimestamp.MaxValue)
  implicit val orderingApproximateTime: Ordering[ApproximateTime] =
    Ordering.by[ApproximateTime, CantonTimestamp](_.value)
}

final case class SequencedTime(value: CantonTimestamp) {
  def toApproximate: ApproximateTime = ApproximateTime(value)

  def toProtoPrimitive: ProtoTimestamp = value.toProtoTimestamp
  def toLf: LfTimestamp = value.toLf
}
object SequencedTime {
  val MinValue: SequencedTime = SequencedTime(CantonTimestamp.MinValue)
  implicit val orderingSequencedTime: Ordering[SequencedTime] =
    Ordering.by[SequencedTime, CantonTimestamp](_.value)
  def fromProtoPrimitive(ts: ProtoTimestamp): ParsingResult[SequencedTime] =
    CantonTimestamp.fromProtoTimestamp(ts).map(SequencedTime(_))
}
