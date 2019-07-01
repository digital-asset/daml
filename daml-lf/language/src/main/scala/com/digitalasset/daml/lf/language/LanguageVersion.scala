// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.language

import com.digitalasset.daml.lf.language.{LanguageMajorVersion => LMV}

final case class LanguageVersion(major: LanguageMajorVersion, minor: LanguageMinorVersion) {
  def pretty: String = s"${major.pretty}.${minor.toProtoIdentifier}"
}

object LanguageVersion {
  type Major = LanguageMajorVersion
  val Major = LanguageMajorVersion

  type Minor = LanguageMinorVersion
  val Minor = LanguageMinorVersion

  val defaultV0: LanguageVersion =
    LanguageVersion(LMV.V0, LMV.V0.maxSupportedStableMinorVersion)

  val defaultV1: LanguageVersion =
    LanguageVersion(LMV.V1, LMV.V1.maxSupportedStableMinorVersion)

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

  /** See <https://github.com/digital-asset/daml/issues/1866>. To not break backwards
    * compatibility, we introduce a new DAML-LF version where this restriction is in
    * place, and then:
    * * When committing a scenario, we check that the scenario code is at least of that
    *   version;
    * * When executing a Ledger API command, we check that the template underpinning
    *   said command is at least of that version.
    */
  val checkSubmitterIsInLookupMaintainers = LanguageVersion(LMV.V1, Minor.Dev)
}
