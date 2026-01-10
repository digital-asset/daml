// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.daml.nonempty.{NonEmpty, NonEmptyUtil}

/** Objects and variables of this file are used as part of the release process. Be mindful when
  * removing them. See:
  *   - `propose.sh` script
  *   - generateReferenceJson
  */
object ReleaseVersionToProtocolVersions {
  private val v2 = ProtocolVersion(2)
  private val v3 = ProtocolVersion(3)
  private val v4 = ProtocolVersion(4)
  private val v5 = ProtocolVersion(5)
  private val v6 = ProtocolVersion(6)
  private val v30 = ProtocolVersion(30)
  private val v31 = ProtocolVersion(31)
  private val v32 = ProtocolVersion(32)
  private val v33 = ProtocolVersion(33)

  import ProtocolVersion.*

  // For each (major, minor) the list of supported protocol versions
  // Don't make this variable private because it's used in `console-reference.canton`
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
      ReleaseVersions.v3_0_0 -> List(v30),
      ReleaseVersions.v3_1_0 -> List(v31),
      ReleaseVersions.v3_2_0 -> List(v32),
      ReleaseVersions.v3_3_0 -> List(v33),
      ReleaseVersions.v3_4_0 -> List(v34),
      ReleaseVersions.v3_5_0_snapshot -> List(v34),
    ).map { case (release, pvs) => (release.majorMinor, NonEmptyUtil.fromUnsafe(pvs)) }

  val majorMinorToBetaProtocolVersions: Map[(Int, Int), NonEmpty[List[ProtocolVersion]]] = Map(
    ReleaseVersions.v2_9_0 -> List(v6)
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

/** This is used in the release process. Do not rename, nor remove the `extends App`
  */
object ReleaseVersionToProtocolVersionsExporter extends App {
  import io.circe.syntax.*
  import better.files.File

  private sealed trait ProtocolVersionStatus {
    def pv: ProtocolVersion
    def isBeta: Boolean = this match {
      case ProtocolVersionStatus.Beta(_) => true
      case _ => false
    }
    override def toString: String = pv.toString
  }

  private object ProtocolVersionStatus {
    implicit def ordering: Ordering[ProtocolVersionStatus] =
      Ordering.by(_.pv)
    final case class Beta(pv: ProtocolVersion) extends ProtocolVersionStatus
    final case class Stable(pv: ProtocolVersion) extends ProtocolVersionStatus
  }

  private val stableMap: Map[(Int, Int), NonEmpty[List[ProtocolVersionStatus.Stable]]] =
    ReleaseVersionToProtocolVersions.majorMinorToStableProtocolVersions.view
      .mapValues(_.map(ProtocolVersionStatus.Stable(_)))
      .toMap

  private val betaMap = ReleaseVersionToProtocolVersions.majorMinorToBetaProtocolVersions.view
    .mapValues(_.map(ProtocolVersionStatus.Beta(_)))
    .toMap

  // For each (major, minor) the list of stable and beta protocol versions
  private val majorMinorToProtocolVersions: Map[(Int, Int), List[ProtocolVersionStatus]] =
    stableMap.foldLeft[Map[(Int, Int), List[ProtocolVersionStatus]]](betaMap) {
      case (acc, (releaseVersion, protocolVersions)) =>
        acc + (releaseVersion -> (acc.getOrElse(releaseVersion, Nil) ++ protocolVersions))
    }

  // (major, minor) -> [protocol version]
  private val stableAndBetaReleasesToProtocolVersions
      : List[((Int, Int), List[ProtocolVersionStatus])] =
    majorMinorToProtocolVersions.toList
      .sortBy { case (releaseVersion, _) => releaseVersion }

  private val releaseVersionsToProtocolVersions: List[(String, List[String])] =
    stableAndBetaReleasesToProtocolVersions
      .collect { case ((major, minor), protocolVersions) =>
        s"$major.$minor" -> protocolVersions.sorted.collect {
          case pv if pv.isBeta => s"${pv.toString}*"
          case pv => pv.toString
        }
      }
      .filter { case (_, protocolVersions) => protocolVersions.nonEmpty }

  File("embed_reference.json").write(releaseVersionsToProtocolVersions.asJson.spaces2)
}
