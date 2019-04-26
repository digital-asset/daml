// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive

import com.digitalasset.daml.lf.archive.{LanguageMajorVersion => LMV}

final case class LanguageVersion(major: LanguageMajorVersion, minor: LanguageMinorVersion)

object LanguageVersion {

  val defaultV0: LanguageVersion =
    LanguageVersion(LMV.V0, LMV.V0.maxSupportedMinorVersion)

  val defaultV1: LanguageVersion =
    LanguageVersion(LMV.V1, LMV.V1.maxSupportedMinorVersion)

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

}
