// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.testing.parser

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.LanguageVersion

case class ParserParameters[P](
    defaultPackageId: PackageId,
    languageVersion: LanguageVersion,
)

object ParserParameters {
  def default[P]: ParserParameters[P] = {
    ParserParameters(
      defaultPackageId = Ref.PackageId.assertFromString("-pkgId-"),
      LanguageVersion.defaultLfVersion,
    )
  }
}
