// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError.ValueConversionError
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.ProtocolVersion
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

final case class PhysicalSynchronizerId(
    logical: SynchronizerId,
    protocolVersion: ProtocolVersion,
    serial: NonNegativeInt,
) extends HasUniqueIdentifier
    with PrettyPrinting {
  def suffix: String = s"$protocolVersion${PhysicalSynchronizerId.secondaryDelimiter}$serial"

  def uid: UniqueIdentifier = logical.uid

  def toLengthLimitedString: String300 =
    String300.tryCreate(
      s"${logical.toLengthLimitedString}${PhysicalSynchronizerId.primaryDelimiter}$suffix"
    )

  def toProtoPrimitive: String = toLengthLimitedString.unwrap

  override protected def pretty: Pretty[PhysicalSynchronizerId.this.type] =
    prettyOfString(_ => toLengthLimitedString.unwrap)
}

object PhysicalSynchronizerId {
  private val primaryDelimiter: String = "::" // Between LSId and suffix
  private val secondaryDelimiter: String = "-" // Between components of the suffix

  def apply(
      synchronizerId: SynchronizerId,
      staticSynchronizerParameters: StaticSynchronizerParameters,
  ): PhysicalSynchronizerId =
    PhysicalSynchronizerId(
      synchronizerId,
      staticSynchronizerParameters.protocolVersion,
      staticSynchronizerParameters.serial,
    )

  implicit val physicalSynchronizerIdOrdering: Ordering[PhysicalSynchronizerId] =
    Ordering.by(psid =>
      (psid.logical.toLengthLimitedString.unwrap, psid.protocolVersion, psid.serial)
    )

  def fromString(raw: String): Either[String, PhysicalSynchronizerId] = {
    val elements = raw.split(primaryDelimiter)
    val elementsCount = elements.sizeIs

    if (elementsCount == 3) {
      for {
        lsid <- SynchronizerId.fromString(elements.take(2).mkString(primaryDelimiter))
        suffix = elements(2)
        suffixComponents = suffix.split("-")
        _ <- Either.cond(
          suffixComponents.sizeIs == 2,
          (),
          s"Cannot parse $suffix as a physical synchronizer id suffix",
        )
        pv <- ProtocolVersion.create(suffixComponents(0))
        serialInt <- suffixComponents(1).toIntOption.toRight(
          s"Cannot parse ${suffixComponents(1)} to an int"
        )
        serial <- NonNegativeInt.create(serialInt).leftMap(_.message)
      } yield PhysicalSynchronizerId(lsid, pv, serial)
    } else
      Left(s"Unable to parse `$raw` as physical synchronizer id")
  }

  def fromProtoPrimitive(proto: String, field: String): ParsingResult[PhysicalSynchronizerId] =
    fromString(proto).leftMap(ValueConversionError(field, _))

  def tryFromString(raw: String): PhysicalSynchronizerId =
    fromString(raw).valueOr(err => throw new IllegalArgumentException(err))

  implicit val getResultSynchronizerId: GetResult[PhysicalSynchronizerId] = GetResult { r =>
    tryFromString(r.nextString())
  }

  implicit val getResultSynchronizerIdO: GetResult[Option[PhysicalSynchronizerId]] =
    GetResult { r =>
      r.nextStringOption().map(tryFromString)
    }

  implicit val setParameterSynchronizerId: SetParameter[PhysicalSynchronizerId] =
    (d: PhysicalSynchronizerId, pp: PositionedParameters) => pp >> d.toLengthLimitedString.unwrap
  implicit val setParameterSynchronizerIdO: SetParameter[Option[PhysicalSynchronizerId]] =
    (d: Option[PhysicalSynchronizerId], pp: PositionedParameters) =>
      pp >> d.map(_.toLengthLimitedString.unwrap)
}

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
