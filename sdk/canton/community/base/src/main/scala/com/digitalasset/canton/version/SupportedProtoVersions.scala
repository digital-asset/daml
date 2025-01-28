// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import cats.syntax.functor.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.ProtoDeserializationError.UnknownProtoVersion
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.{ProtoDeserializationError, checked}

import scala.collection.immutable
import scala.collection.immutable.SortedMap
import scala.math.Ordered.orderingToOrdered

final case class SupportedProtoVersions[
    ValueClass,
    Context,
    DeserializedValueClass,
    Comp,
    Dependency,
] private (
    // Sorted with descending order
    converters: NonEmpty[immutable.SortedMap[
      ProtoVersion,
      ProtoCodec[
        ValueClass,
        Context,
        DeserializedValueClass,
        Comp,
        Dependency,
      ],
    ]],
    name: String,
) {

  type Codec = ProtoCodec[
    ValueClass,
    Context,
    DeserializedValueClass,
    Comp,
    Dependency,
  ]

  val (higherProtoVersion, higherConverter) = converters.head1

  type Deserializer =
    (Context, OriginalByteString, DataByteString) => ParsingResult[DeserializedValueClass]

  def converterFor(
      protocolVersion: ProtocolVersion
  ): Codec =
    converters
      .collectFirst {
        case (_, converter) if protocolVersion >= converter.fromInclusive.representative =>
          converter
      }
      .getOrElse(higherConverter)

  def deserializerFor(
      protoVersion: ProtoVersion
  ): Deserializer =
    converters.get(protoVersion).map(_.deserializer).getOrElse(higherConverter.deserializer)

  def protoVersionFor(
      protocolVersion: RepresentativeProtocolVersion[Comp]
  ): ProtoVersion = converters
    .collectFirst {
      case (protoVersion, converter) if protocolVersion >= converter.fromInclusive =>
        protoVersion
    }
    .getOrElse(higherProtoVersion)

  def converterFor(
      protocolVersion: RepresentativeProtocolVersion[Comp]
  ): ParsingResult[Codec] = converters
    .collectFirst {
      case (_, converter) if protocolVersion >= converter.fromInclusive =>
        converter
    }
    .toRight(
      ProtoDeserializationError.OtherError(
        s"Unable to find code for representative protocol version $protocolVersion"
      )
    )

  def protocolVersionRepresentativeFor(
      protoVersion: ProtoVersion
  ): ParsingResult[RepresentativeProtocolVersion[Comp]] =
    table.get(protoVersion).toRight(UnknownProtoVersion(protoVersion, name))

  def protocolVersionRepresentativeFor(
      protocolVersion: ProtocolVersion
  ): RepresentativeProtocolVersion[Comp] = converterFor(
    protocolVersion
  ).fromInclusive

  lazy val table: Map[ProtoVersion, RepresentativeProtocolVersion[Comp]] =
    converters.forgetNE.fmap(_.fromInclusive)
}

object SupportedProtoVersions {

  def apply[
      ValueClass,
      Context,
      DeserializedValueClass,
      Comp,
      Dependency,
  ](name: String)(
      head: (
          ProtoVersion,
          ProtoCodec[
            ValueClass,
            Context,
            DeserializedValueClass,
            Comp,
            Dependency,
          ],
      ),
      tail: (
          ProtoVersion,
          ProtoCodec[
            ValueClass,
            Context,
            DeserializedValueClass,
            Comp,
            Dependency,
          ],
      )*
  ): SupportedProtoVersions[
    ValueClass,
    Context,
    DeserializedValueClass,
    Comp,
    Dependency,
  ] =
    SupportedProtoVersions.fromNonEmpty(name)(
      NonEmpty.mk(Seq, head, tail*)
    )

  /*
   Throws an error if a protocol version or a protobuf version is used twice.
   This indicates an error in the converters list:
   - Each protobuf version should appear only once.
   - Each protobuf version should use a different minimum protocol version.
   */
  private def ensureNoDuplicates(
      converters: NonEmpty[Seq[(ProtoVersion, ProtoCodec[?, ?, ?, ?, ?])]],
      name: String,
  ): Unit = {

    val versions: Seq[(ProtoVersion, ProtocolVersion)] = converters.forgetNE.map {
      case (protoVersion, codec) =>
        (protoVersion, codec.fromInclusive.representative)
    }

    def getDuplicates[T](
        proj: ((ProtoVersion, ProtocolVersion)) => T
    ): Option[NonEmpty[List[T]]] = {
      val duplicates = versions
        .groupBy(proj)
        .toList
        .collect {
          case (_, groupedVersions) if groupedVersions.lengthCompare(1) > 0 =>
            groupedVersions.map(proj)
        }
        .flatten

      NonEmpty.from(duplicates)
    }

    val duplicatedProtoVersion = getDuplicates(_._1)
    val duplicatedProtocolVersion = getDuplicates(_._2)

    duplicatedProtoVersion.foreach { duplicates =>
      throw new IllegalArgumentException(
        s"Some protobuf versions appear several times in `$name`: $duplicates"
      )
    }.discard

    duplicatedProtocolVersion.foreach { duplicates =>
      throw new IllegalArgumentException(
        s"Some protocol versions appear several times in `$name`: $duplicates"
      )
    }.discard
  }

  private[version] def fromNonEmpty[
      ValueClass,
      Context,
      DeserializedValueClass,
      Comp,
      Dependency,
  ](name: String)(
      converters: NonEmpty[Seq[
        (
            ProtoVersion,
            ProtoCodec[
              ValueClass,
              Context,
              DeserializedValueClass,
              Comp,
              Dependency,
            ],
        )
      ]]
  ): SupportedProtoVersions[
    ValueClass,
    Context,
    DeserializedValueClass,
    Comp,
    Dependency,
  ] = {
    ensureNoDuplicates(converters, name)

    val sortedConverters: NonEmpty[SortedMap[ProtoVersion, ProtoCodec[
      ValueClass,
      Context,
      DeserializedValueClass,
      Comp,
      Dependency,
    ]]] = checked(
      NonEmptyUtil.fromUnsafe(
        immutable.SortedMap.from(converters)(implicitly[Ordering[ProtoVersion]].reverse)
      )
    )
    val (_, lowestProtocolVersion) = sortedConverters.last1

    // If you are hitting this require failing when your message doesn't exist in PV.minimum,
    // remember to specify that explicitly by adding to the SupportedProtoVersions:
    // ProtoVersion(-1) -> UnsupportedProtoCodec(ProtocolVersion.minimum),
    require(
      lowestProtocolVersion.fromInclusive.representative == ProtocolVersion.minimum,
      s"ProtocolVersion corresponding to lowest proto version should be ${ProtocolVersion.minimum}, found $lowestProtocolVersion",
    )

    SupportedProtoVersions(sortedConverters, name)
  }
}
