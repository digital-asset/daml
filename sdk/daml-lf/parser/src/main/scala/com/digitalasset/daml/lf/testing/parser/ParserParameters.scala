// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.testing.parser

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.{LanguageMajorVersion, LanguageVersion}

case class ParserParameters[P](
    defaultPackageId: PackageId,
    languageVersion: LanguageVersion,
)

object ParserParameters {

  def defaultFor[P](majorLanguageVersion: LanguageMajorVersion): ParserParameters[P] = {
    majorLanguageVersion match {
      case LanguageMajorVersion.V1 =>
        throw new IllegalArgumentException("Lf1 is not supported")
      case LanguageMajorVersion.V2 =>
        ParserParameters(
          defaultPackageId = Ref.PackageId.assertFromString("-pkgId-"),
          LanguageVersion.defaultOrLatestStable(LanguageMajorVersion.V2),
        )
    }
  }
}
