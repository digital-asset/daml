// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.testing.parser

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.{LanguageMajorVersion, LanguageVersion}

private[daml] case class ParserParameters[P](
    defaultPackageId: PackageId,
    languageVersion: LanguageVersion,
)

private[daml] object ParserParameters {

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
