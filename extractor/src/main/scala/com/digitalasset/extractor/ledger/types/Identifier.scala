// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.ledger.types

import com.digitalasset.ledger.api.{v1 => api}

import scalaz._
import Scalaz._

final case class Identifier(packageId: String, name: String)

object Identifier {
  final implicit class ApiIdentifierOps(val apiIdentifier: api.value.Identifier) extends AnyVal {
    // Even if it couldn't return a `Left`, making it return an `Either` for the sake of consistency
    def convert: String \/ Identifier =
      Identifier(
        apiIdentifier.packageId,
        apiIdentifier.moduleName + ":" + apiIdentifier.entityName
      ).right
  }
}
