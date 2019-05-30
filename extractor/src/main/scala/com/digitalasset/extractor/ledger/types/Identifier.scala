// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.ledger.types

import com.digitalasset.ledger.api.{v1 => api}

final case class Identifier(packageId: String, name: String)

object Identifier {
  private val separator: Char = ':'

  final implicit class ApiIdentifierOps(val apiIdentifier: api.value.Identifier) extends AnyVal {
    def convert: Identifier =
      Identifier(
        apiIdentifier.packageId,
        apiIdentifier.moduleName + separator.toString + apiIdentifier.entityName
      )
  }
}
