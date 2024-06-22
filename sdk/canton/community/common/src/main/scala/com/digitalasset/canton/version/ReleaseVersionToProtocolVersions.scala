// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.daml.nonempty.{NonEmpty, NonEmptyUtil}

object ReleaseVersionToProtocolVersions {
  private val v2 = ProtocolVersion(2)

  import ProtocolVersion.*

  val majorMinorToStableProtocolVersions: Map[(Int, Int), NonEmpty[List[ProtocolVersion]]] =
    Map(
      ReleaseVersions.v2_0_0 -> List(v2),
      ReleaseVersions.v2_1_0 -> List(v2),
      ReleaseVersions.v2_2_0 -> List(v2),
      ReleaseVersions.v2_3_0 -> List(v2, v3),
      ReleaseVersions.v2_4_0 -> List(v2, v3),
      ReleaseVersions.v2_5_0 -> List(v2, v3, v4),
      ReleaseVersions.v2_6_0 -> List(v3, v4),
      ReleaseVersions.v2_7_0 -> List(v3, v4, v5),
      ReleaseVersions.v2_8_0 -> List(v3, v4, v5),
      ReleaseVersions.v2_9_0 -> List(v5),
      ReleaseVersions.v2_10_0 -> List(v5),
    ).map { case (release, pvs) => (release.majorMinor, NonEmptyUtil.fromUnsafe(pvs)) }

  val majorMinorToBetaProtocolVersions: Map[(Int, Int), NonEmpty[List[ProtocolVersion]]] = Map(
    ReleaseVersions.v2_9_0 -> List(v6),
    ReleaseVersions.v2_10_0 -> List(v6),
  ).map { case (release, pvs) => (release.majorMinor, NonEmptyUtil.fromUnsafe(pvs)) }

  def getOrElse(
      releaseVersion: ReleaseVersion,
      default: => NonEmpty[List[ProtocolVersion]],
  ): NonEmpty[List[ProtocolVersion]] =
    majorMinorToStableProtocolVersions.getOrElse(releaseVersion.majorMinor, default)

  def getBetaProtocolVersions(releaseVersion: ReleaseVersion): List[ProtocolVersion] =
    majorMinorToBetaProtocolVersions
      .get(releaseVersion.majorMinor)
      .map(_.forgetNE)
      .getOrElse(Nil)
}
