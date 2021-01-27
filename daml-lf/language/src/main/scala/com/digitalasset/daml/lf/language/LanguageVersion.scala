// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.language

import com.daml.lf.VersionRange

final case class LanguageVersion(major: LanguageMajorVersion, minor: LanguageMinorVersion) {
  def pretty: String = s"${major.pretty}.${minor.toProtoIdentifier}"
}

object LanguageVersion {

  type Major = LanguageMajorVersion
  val Major = LanguageMajorVersion

  type Minor = LanguageMinorVersion
  val Minor = LanguageMinorVersion

  implicit val Ordering: scala.Ordering[LanguageVersion] = {
    case (LanguageVersion(Major.V1, leftMinor), LanguageVersion(Major.V1, rightMinor)) =>
      Major.V1.minorVersionOrdering.compare(leftMinor, rightMinor)
  }

  private[lf] val All = Major.V1.supportedMinorVersions.map(LanguageVersion(Major.V1, _))

  private[lf] val List(v1_6, v1_7, v1_8, v1_11, v1_12, v1_dev) = All

  object Features {
    val default = v1_6
    val textPacking = v1_6
    val enum = v1_6
    val internedPackageId = v1_6
    val internedStrings = v1_7
    val internedDottedNames = v1_7
    val numeric = v1_7
    val anyType = v1_7
    val typeRep = v1_7
    val typeSynonyms = v1_8
    val packageMetadata = v1_8
    val genComparison = v1_11
    val genMap = v1_11
    val scenarioMustFailAtMsg = v1_11
    val contractIdTextConversions = v1_11
    val exerciseByKey = v1_11
    val internedTypes = v1_11
    val choiceObservers = v1_11
    val exceptions = v1_dev

    /** Unstable, experimental features. This should stay in 1.dev forever.
      * Features implemented with this flag should be moved to a separate
      * feature flag once the decision to add them permanently has been made.
      */
    val unstable = v1_dev

  }

  // All the stable versions.
  val StableVersions: VersionRange[LanguageVersion] =
    VersionRange(min = v1_6, max = v1_11)

  // All versions compatible with legacy contract ID scheme.
  val LegacyVersions: VersionRange[LanguageVersion] =
    StableVersions.copy(max = v1_8)

  // All the stable and preview versions
  // Equals `Stable` if no preview version is available
  val EarlyAccessVersions: VersionRange[LanguageVersion] =
    StableVersions.copy(max = v1_12)

  // All the versions
  val DevVersions: VersionRange[LanguageVersion] =
    StableVersions.copy(max = v1_dev)

  val defaultV1: LanguageVersion = StableVersions.max

  val default: LanguageVersion = defaultV1

}
