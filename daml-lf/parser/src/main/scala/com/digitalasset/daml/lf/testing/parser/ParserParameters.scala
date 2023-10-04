// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.testing.parser

import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.{LanguageMajorVersion, LanguageVersion}

private[daml] case class ParserParameters[P](
    defaultPackageId: PackageId,
    languageVersion: LanguageVersion,
)

private[daml] object ParserParameters {

  def defaultFor[P](majorLanguageVersion: LanguageMajorVersion): ParserParameters[P] =
    ParserParameters(
      defaultPackageId = com.daml.lf.testing.parser.defaultPackageId,
      // TODO(#17366): use something like LanguageVersion.default(major) after the refactoring of
      //  LanguageVersion
      majorLanguageVersion match {
        case LanguageMajorVersion.V1 => com.daml.lf.testing.parser.defaultLanguageVersion
        case LanguageMajorVersion.V2 => LanguageVersion.v2_dev
      },
    )
}
