// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.http.Generators.{genApiIdentifier, genDuplicateApiIdentifiers}
import com.digitalasset.ledger.api.v1.value.Identifier
import org.scalacheck.Gen.nonEmptyListOf
import org.scalacheck.Shrink
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Inside, Matchers}
import scalaz.{-\/, \/-}

class PackageServiceTest
    extends FlatSpec
    with Matchers
    with Inside
    with GeneratorDrivenPropertyChecks {

  import Shrink.shrinkAny

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  behavior of "PackageService.buildMap"

  it should "discard and report identifiers with the same (moduleName, entityName) as duplicates" in
    forAll(genDuplicateApiIdentifiers) { ids =>
      val (dups, map) = PackageService.buildMap(ids.toSet)
      PackageService.fold(dups) shouldBe ids.toSet
      map shouldBe Map.empty
    }

  it should "discard 2 identifiers with the same (moduleName, entityName) as duplicates" in
    forAll(genApiIdentifier) { id0 =>
      val id1 = id0.copy(packageId = id0.packageId + "aaaa")
      val (dups, map) = PackageService.buildMap(Set(id0, id1))
      PackageService.fold(dups) shouldBe Set(id0, id1)
      map shouldBe Map.empty
    }

  it should "pass one specific test case that was failing" in {
    val id0 = Identifier("a", "", "f4", "x")
    val id1 = Identifier("b", "", "f4", "x")
    val (dups, map) = PackageService.buildMap(Set(id0, id1))
    (PackageService.fold(dups), map) shouldBe (Set(id0, id1) -> Map())
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

  behavior of "PackageService.resolveTemplateIds"

  it should "return all API Identifier by moduleName and entityName" in forAll(
    nonEmptyListOf(genApiIdentifier)) { ids =>
    val (_, map) = PackageService.buildMap(ids.toSet)
    val uniqueIds: Set[Identifier] = map.values.toSet
    val uniqueDomainIds: Set[domain.TemplateId.OptionalPkg] = uniqueIds.map(x =>
      domain.TemplateId(packageId = None, moduleName = x.moduleName, entityName = x.entityName))

    inside(PackageService.resolveTemplateIds(map)(uniqueDomainIds)) {
      case \/-(actualIds) => actualIds.toSet shouldBe uniqueIds
    }
  }

  it should "return error for unmapped ID" in forAll(
    nonEmptyListOf(genApiIdentifier),
    nonEmptyListOf(genApiIdentifier)) { (knownIds, otherIds) =>
    val (_, map) = PackageService.buildMap(knownIds.toSet)

    val unknownIds: Set[Identifier] = otherIds.toSet diff knownIds.toSet
    val unknownDomainIds: Set[domain.TemplateId.OptionalPkg] = unknownIds.map(x =>
      domain.TemplateId(packageId = None, moduleName = x.moduleName, entityName = x.entityName))

    unknownDomainIds.foreach { id =>
      inside(PackageService.resolveTemplateId(map)(id)) {
        case -\/(e) =>
          val t = (id.moduleName, id.entityName)
          e shouldBe s"Cannot resolve ${t.toString}"
      }
    }

    inside(PackageService.resolveTemplateIds(map)(unknownDomainIds)) {
      case -\/(e) => e should startWith("Cannot resolve ")
    }
  }
}
