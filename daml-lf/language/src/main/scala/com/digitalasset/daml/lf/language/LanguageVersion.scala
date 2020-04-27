// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.language

final case class LanguageVersion(major: LanguageMajorVersion, minor: LanguageMinorVersion) {
  def pretty: String = s"${major.pretty}.${minor.toProtoIdentifier}"
}

object LanguageVersion {

  type Major = LanguageMajorVersion
  val Major = LanguageMajorVersion

  type Minor = LanguageMinorVersion
  val Minor = LanguageMinorVersion

  val defaultV0: LanguageVersion =
    LanguageVersion(Major.V0, Major.V0.maxSupportedStableMinorVersion)

  val defaultV1: LanguageVersion =
    LanguageVersion(Major.V1, Major.V1.maxSupportedStableMinorVersion)

  private[lf] def apply(major: LanguageMajorVersion, minor: String): LanguageVersion =
    apply(major, Minor fromProtoIdentifier minor)

  val default: LanguageVersion =
    defaultV1

  final val ordering: Ordering[LanguageVersion] =
    (left, right) =>
      (left, right) match {
        case (LanguageVersion(leftMajor, leftMinor), LanguageVersion(rightMajor, rightMinor))
            if leftMajor == rightMajor =>
          leftMajor.minorVersionOrdering.compare(leftMinor, rightMinor)
        case (LanguageVersion(leftMajor, _), LanguageVersion(rightMajor, _)) =>
          LanguageMajorVersion.ordering.compare(leftMajor, rightMajor)
    }
  object Features {

    private val List(v1_0, v1_1, v1_2, v1_3, v1_4, v1_5, v1_6, v1_7, v1_8, v1_dev) =
      Major.V1.supportedMinorVersions.map(LanguageVersion(Major.V1, _))

    val default = v1_0
    val arrowType = v1_1
    val optional = v1_1
    val partyOrdering = v1_1
    val partyTextConversions = v1_2
    val shaText = v1_2
    val contractKeys = v1_3
    val textMap = v1_3
    val complexContactKeys = v1_4
    val optionalExerciseActor = v1_5
    val numberParsing = v1_5
    val coerceContractId = v1_5
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
    val genComparison = v1_dev
    val genMap = v1_dev
    val scenarioMustFailAtMsg = v1_dev

    /** Unstable, experimental features. This should stay in 1.dev forever.
      * Features implemented with this flag should be moved to a separate
      * feature flag once the decision to add them permanently has been made.
      */
    val unstable = v1_dev

  }
}
