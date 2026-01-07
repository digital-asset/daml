// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

/*
Before the first handshake with a synchronizer, the physical synchronizer id is unknown.
This trait allows for an explicit representation of `Option[PhysicalSynchronizerId]`.
 */
sealed trait ConfiguredPhysicalSynchronizerId extends PrettyPrinting {
  def toOption: Option[PhysicalSynchronizerId]
  def isDefined: Boolean = toOption.isDefined
}

final case class KnownPhysicalSynchronizerId(psid: PhysicalSynchronizerId)
    extends ConfiguredPhysicalSynchronizerId {
  override protected def pretty: Pretty[KnownPhysicalSynchronizerId] =
    prettyOfString(psid => psid.psid.toLengthLimitedString.unwrap)

  override def toOption: Option[PhysicalSynchronizerId] = Some(psid)
}
case object UnknownPhysicalSynchronizerId extends ConfiguredPhysicalSynchronizerId {
  override protected def pretty: Pretty[UnknownPhysicalSynchronizerId.type] =
    prettyOfString(_ => "UnknownPhysicalSynchronizerId")

  override def toOption: Option[PhysicalSynchronizerId] = None
}

object ConfiguredPhysicalSynchronizerId {
  def apply(psid: Option[PhysicalSynchronizerId]): ConfiguredPhysicalSynchronizerId =
    psid.fold[ConfiguredPhysicalSynchronizerId](UnknownPhysicalSynchronizerId)(
      KnownPhysicalSynchronizerId(_)
    )

  implicit val getResultConfiguredPhysicalSynchronizerId
      : GetResult[ConfiguredPhysicalSynchronizerId] =
    PhysicalSynchronizerId.getResultSynchronizerIdO.andThen(ConfiguredPhysicalSynchronizerId.apply)

  implicit val setParameterConfiguredPhysicalSynchronizerId
      : SetParameter[ConfiguredPhysicalSynchronizerId] =
    (psid: ConfiguredPhysicalSynchronizerId, pp: PositionedParameters) => pp >> psid.toOption

  implicit val configuredPSIdOrdering: Ordering[ConfiguredPhysicalSynchronizerId] =
    Ordering.by(_.toOption)
}
