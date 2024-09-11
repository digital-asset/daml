// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.kernel.Order
import com.digitalasset.canton.data.ViewType
import com.digitalasset.canton.data.ViewType.{AssignmentViewType, UnassignmentViewType}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.ReassignmentDomainId.ReassignmentDomainIdCast
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.DomainId
import slick.jdbc.{PositionedParameters, SetParameter}

/** This trait can be used when distinction between source and target domain is important.
  */
sealed trait ReassignmentDomainId extends PrettyPrinting with Product with Serializable {
  def unwrap: DomainId

  def toProtoPrimitive: String = unwrap.toProtoPrimitive

  def toViewType: ViewType

  override def pretty: Pretty[this.type] = prettyOfParam(_.unwrap)
}

object ReassignmentDomainId {
  implicit val orderReassignmentDomainId: Order[ReassignmentDomainId] =
    Order.by[ReassignmentDomainId, String](_.toProtoPrimitive)

  implicit val setParameterReassignmentDomainId: SetParameter[ReassignmentDomainId] =
    (d: ReassignmentDomainId, pp: PositionedParameters) => pp >> d.unwrap.toLengthLimitedString
  implicit val setParameterReassignmentDomainIdO: SetParameter[Option[ReassignmentDomainId]] =
    (d: Option[ReassignmentDomainId], pp: PositionedParameters) =>
      pp >> d.map(_.unwrap.toLengthLimitedString)

  trait ReassignmentDomainIdCast[Kind <: ReassignmentDomainId] {
    def toKind(domain: ReassignmentDomainId): Option[Kind]
  }

  implicit val reassignmentDomainIdCast: ReassignmentDomainIdCast[ReassignmentDomainId] =
    (domain: ReassignmentDomainId) => Some(domain)
}

final case class SourceDomainId(id: DomainId) extends ReassignmentDomainId {
  override def unwrap: DomainId = id

  override def toViewType: UnassignmentViewType = UnassignmentViewType
}

object SourceDomainId {
  implicit val orderSourceDomainId: Order[SourceDomainId] =
    Order.by[SourceDomainId, String](_.toProtoPrimitive)

  implicit val sourceDomainIdCast: ReassignmentDomainIdCast[SourceDomainId] = {
    case x: SourceDomainId => Some(x)
    case _ => None
  }

  def fromProtoPrimitive(
      proto: String,
      fieldName: String,
  ): ParsingResult[SourceDomainId] =
    DomainId.fromProtoPrimitive(proto, fieldName).map(SourceDomainId(_))
}

final case class TargetDomainId(id: DomainId) extends ReassignmentDomainId {
  override def unwrap: DomainId = id

  override def toViewType: AssignmentViewType = AssignmentViewType

}

object TargetDomainId {
  implicit val targetDomainIdCast: ReassignmentDomainIdCast[TargetDomainId] = {
    case x: TargetDomainId => Some(x)
    case _ => None
  }

  def fromProtoPrimitive(
      proto: String,
      fieldName: String,
  ): ParsingResult[TargetDomainId] =
    DomainId.fromProtoPrimitive(proto, fieldName).map(TargetDomainId(_))
}
