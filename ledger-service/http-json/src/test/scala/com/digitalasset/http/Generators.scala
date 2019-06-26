// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.ledger.api.{v1 => API}
import org.scalacheck.Gen

object Generators {
  def genApiIdentifier: Gen[API.value.Identifier] =
    for {
      p <- Gen.identifier
      m <- Gen.identifier
      e <- Gen.identifier
    } yield API.value.Identifier(packageId = p, moduleName = m, entityName = e)

  def genDuplicateApiIdentifiers: Gen[List[API.value.Identifier]] =
    for {
      id0 <- genApiIdentifier
      otherPackageIds <- Gen.nonEmptyListOf(Gen.identifier)
    } yield id0 :: otherPackageIds.map(a => id0.copy(packageId = a))
}
