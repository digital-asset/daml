// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.kernel.Order
import com.digitalasset.canton.data.ViewType
import com.digitalasset.canton.data.ViewType.{TransferInViewType, TransferOutViewType}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.TransferDomainId.TransferDomainIdCast
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.DomainId
import slick.jdbc.{PositionedParameters, SetParameter}

/** This trait can be used when distinction between source and target domain is important.
  */
sealed trait TransferDomainId extends PrettyPrinting with Product with Serializable {
  def unwrap: DomainId

  def toProtoPrimitive: String = unwrap.toProtoPrimitive

  def toViewType: ViewType

  override def pretty: Pretty[this.type] = prettyOfParam(_.unwrap)
}

object TransferDomainId {
  implicit val orderTransferDomainId: Order[TransferDomainId] =
    Order.by[TransferDomainId, String](_.toProtoPrimitive)

  implicit val setParameterTransferDomainId: SetParameter[TransferDomainId] =
    (d: TransferDomainId, pp: PositionedParameters) => pp >> d.unwrap.toLengthLimitedString
  implicit val setParameterTransferDomainIdO: SetParameter[Option[TransferDomainId]] =
    (d: Option[TransferDomainId], pp: PositionedParameters) =>
      pp >> d.map(_.unwrap.toLengthLimitedString)

  trait TransferDomainIdCast[Kind <: TransferDomainId] {
    def toKind(domain: TransferDomainId): Option[Kind]
  }

  implicit val transferDomainIdCast: TransferDomainIdCast[TransferDomainId] =
    (domain: TransferDomainId) => Some(domain)
}

final case class SourceDomainId(id: DomainId) extends TransferDomainId {
  override def unwrap: DomainId = id

  override def toViewType: TransferOutViewType = TransferOutViewType
}

object SourceDomainId {
  implicit val orderSourceDomainId: Order[SourceDomainId] =
    Order.by[SourceDomainId, String](_.toProtoPrimitive)

  implicit val sourceDomainIdCast: TransferDomainIdCast[SourceDomainId] = {
    case x: SourceDomainId => Some(x)
    case _ => None
  }

  def fromProtoPrimitive(
      proto: String,
      fieldName: String,
  ): ParsingResult[SourceDomainId] =
    DomainId.fromProtoPrimitive(proto, fieldName).map(SourceDomainId(_))
}

final case class TargetDomainId(id: DomainId) extends TransferDomainId {
  override def unwrap: DomainId = id

  override def toViewType: TransferInViewType = TransferInViewType

}

object TargetDomainId {
  implicit val targetDomainIdCast: TransferDomainIdCast[TargetDomainId] = {
    case x: TargetDomainId => Some(x)
    case _ => None
  }

  def fromProtoPrimitive(
      proto: String,
      fieldName: String,
  ): ParsingResult[TargetDomainId] =
    DomainId.fromProtoPrimitive(proto, fieldName).map(TargetDomainId(_))
}
