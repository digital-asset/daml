// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.http.Generators.{genApiIdentifier, genDuplicateApiIdentifiers}
import org.scalacheck.Gen.nonEmptyListOf
import org.scalacheck.Shrink
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
class PackageServiceTest extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks {

  import Shrink.shrinkAny

  behavior of "PackageService.buildMap"

  it should "discard and report identifiers with the same (moduleName, entityName) as duplicates" in
    forAll(genDuplicateApiIdentifiers) { ids =>
      val (dups, map) = PackageService.buildMap(ids.toSet)
      PackageService.fold(dups) shouldBe ids.toSet
      map shouldBe Map.empty
    }

  it should "discard 3 identifiers with the same (moduleName, entityName) as duplicates" in
    forAll(genApiIdentifier) { id0 =>
      val id1 = id0.copy(packageId = id0.packageId + "aaaa")
      val id2 = id0.copy(packageId = id0.packageId + "bbbb")
      val (dups, map) = PackageService.buildMap(Set(id0, id1, id2))
      PackageService.fold(dups) shouldBe Set(id0, id1, id2)
      map shouldBe Map.empty
    }

  it should "the sum of dups and mappings should equal to the original set of IDs" in
    forAll(nonEmptyListOf(Generators.genApiIdentifier), genDuplicateApiIdentifiers) { (xs, ys) =>
      val ids = (xs ++ ys).toSet
      val (dups, map) = PackageService.buildMap(ids)
      val foldedDups = PackageService.fold(dups)
      foldedDups.size should be >= 2
      foldedDups.size + map.size shouldBe ids.size
      foldedDups ++ map.values shouldBe ids
    }

//  it should "resolve all API Identifier by moduleName and entityName one at a time" in forAll(
//    listOf(genApiIdentifier)) { ids =>
//    val (dup, map) = PackageService.buildMap(ids.toSet)
//
//  }

}
