// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.testing.parser

import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.LanguageVersion

private[digitalasset] case class ParserParameters[P](
    defaultPackageId: PackageId,
    languageVersion: LanguageVersion
)
