// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError.{FieldNotSet, UnrecognizedEnum}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

import scala.collection.immutable.{SortedMap, SortedSet}

sealed abstract class HashingSchemeVersion(val index: Int) {
  def toProtoV30: v30.ExternalAuthorization.HashingSchemeVersion =
    v30.ExternalAuthorization.HashingSchemeVersion.fromValue(index)
}

object HashingSchemeVersion {
  final case object V2 extends HashingSchemeVersion(2)
  implicit val hashingSchemeVersionOrdering: Ordering[HashingSchemeVersion] =
    Ordering.by(_.index)

  /*
    Entries (pv=34 -> V2), (pv=37 -> V2, V3) means
      - pv=34, 35 and 36 support V2
      - pv=37 and onwards support V2 and V3
   */
  private[canton] val MinimumProtocolVersionToHashingVersion =
    SortedMap[ProtocolVersion, NonEmpty[SortedSet[HashingSchemeVersion]]](
      ProtocolVersion.v34 -> NonEmpty.mk(SortedSet, V2)
    )

  def minProtocolVersionForHSV(version: HashingSchemeVersion): Option[ProtocolVersion] =
    MinimumProtocolVersionToHashingVersion.iterator.collectFirst {
      case (pv, isVersions) if isVersions.contains(version) => pv
    }

  def getHashingSchemeVersionsForProtocolVersion(
      protocolVersion: ProtocolVersion
  ): NonEmpty[SortedSet[HashingSchemeVersion]] =
    MinimumProtocolVersionToHashingVersion
      .filter { case (pv, _) => pv <= protocolVersion }
      .maxByOption { case (pv, _) => pv }
      .getOrElse(
        throw new IllegalArgumentException(
          s"Unable to find hashing scheme for protocol version $protocolVersion"
        )
      )
      ._2

  def fromProtoV30(
      version: v30.ExternalAuthorization.HashingSchemeVersion
  ): ParsingResult[HashingSchemeVersion] = version match {
    case v30.ExternalAuthorization.HashingSchemeVersion.HASHING_SCHEME_VERSION_V2 => Right(V2)
    case v30.ExternalAuthorization.HashingSchemeVersion.HASHING_SCHEME_VERSION_UNSPECIFIED =>
      Left(FieldNotSet("hashing_scheme_version"))
    case v30.ExternalAuthorization.HashingSchemeVersion.Unrecognized(unrecognizedValue) =>
      Left(UnrecognizedEnum("hashing_scheme_version", unrecognizedValue))
  }
}
