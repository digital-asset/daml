package com.digitalasset.daml.lf.testing.parser

import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.LanguageVersion

case class ParserParameters[P](
    defaultPackageId: PackageId,
    languageVersion: LanguageVersion
)
