// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.language

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

  def default: LanguageVersion =
    defaultV1

  def ordering: Ordering[LanguageVersion] =
    (left, right) =>
      (left, right) match {
        case (LanguageVersion(leftMajor, leftMinor), LanguageVersion(rightMajor, rightMinor))
            if leftMajor == rightMajor =>
          leftMajor.minorVersionOrdering.compare(leftMinor, rightMinor)
        case (LanguageVersion(leftMajor, _), LanguageVersion(rightMajor, _)) =>
          LanguageMajorVersion.ordering.compare(leftMajor, rightMajor)
    }

  val List(v1_0, v1_1, v1_2, v1_3, v1_4, v1_5, v1_6, _) =
    Major.V1.supportedMinorVersions.map(LanguageVersion(Major.V1, _))

  object Features {

    val optionalVersion = v1_1
    val partyOrderingVersion = v1_1
    val mapVersion = v1_3
    val enumVersion = v1_6
    val internedIdsVersion = v1_6

    /** See <https://github.com/digital-asset/daml/issues/1866>. To not break backwards
      * compatibility, we introduce a new DAML-LF version where this restriction is in
      * place, and then:
      * * When committing a scenario, we check that the scenario code is at least of that
      * version;
      * * When executing a Ledger API command, we check that the template underpinning
      * said command is at least of that version.
      */
    val checkSubmitterInMaintainersVersion = LanguageVersion(Major.V1, Minor.Dev)

  }
}
