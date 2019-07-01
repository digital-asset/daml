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

  def nonEmptySet[A](gen: Gen[A]): Gen[Set[A]] = Gen.nonEmptyListOf(gen).map(_.toSet)

  // Generate Identifiers with unique packageId values, but the same moduleName and entityName.
  def genDuplicateApiIdentifiers: Gen[List[API.value.Identifier]] =
    for {
      id0 <- genApiIdentifier
      otherPackageIds <- nonEmptySet(Gen.identifier.filter(x => x != id0.packageId))
    } yield id0 :: otherPackageIds.map(a => id0.copy(packageId = a)).toList
}
