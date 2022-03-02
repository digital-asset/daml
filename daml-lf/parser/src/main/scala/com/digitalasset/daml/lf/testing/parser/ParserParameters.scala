// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.testing.parser

import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.LanguageVersion

private[daml] case class ParserParameters[P](
    defaultPackageId: PackageId,
    languageVersion: LanguageVersion,
)
