// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.daml.ledger.api.v2.interactive.interactive_submission_service as iss
import com.daml.nonempty.NonEmpty

import scala.collection.immutable.{SortedMap, SortedSet}

sealed abstract class HashingSchemeVersion(val index: Int) {
  def toLAPIProto: iss.HashingSchemeVersion =
    iss.HashingSchemeVersion.fromValue(index)
}

object HashingSchemeVersion {
  final case object V1 extends HashingSchemeVersion(1)
  implicit val hashingSchemeVersionOrdering: Ordering[HashingSchemeVersion] =
    Ordering.by(_.index)

  private val ProtocolVersionToHashingVersion =
    SortedMap[ProtocolVersion, NonEmpty[SortedSet[HashingSchemeVersion]]](
      ProtocolVersion.v33 -> NonEmpty.mk(SortedSet, V1),
      ProtocolVersion.dev -> NonEmpty.mk(SortedSet, V1),
    )

  def minProtocolVersionForISV(version: HashingSchemeVersion): Option[ProtocolVersion] =
    ProtocolVersionToHashingVersion.iterator.collectFirst {
      case (pv, isVersions) if isVersions.contains(version) => pv
    }

  def getVersionedHashConstructorFor(
      protocolVersion: ProtocolVersion
  ): NonEmpty[SortedSet[HashingSchemeVersion]] = {
    assert(
      protocolVersion >= ProtocolVersion.v33,
      s"Canton only supports external signing from ProtocolVersions >= ${ProtocolVersion.v33}",
    )
    ProtocolVersionToHashingVersion(protocolVersion)
  }
}
