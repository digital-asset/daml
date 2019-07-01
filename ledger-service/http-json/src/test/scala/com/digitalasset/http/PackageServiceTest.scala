// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.http.Generators.{genApiIdentifier, genDuplicateApiIdentifiers, nonEmptySet}
import com.digitalasset.ledger.api.v1.value.Identifier
import org.scalacheck.Gen.nonEmptyListOf
import org.scalacheck.Shrink
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Inside, Matchers}
import scalaz.{-\/, \/-}

import scala.collection.breakOut

class PackageServiceTest
    extends FlatSpec
    with Matchers
    with Inside
    with GeneratorDrivenPropertyChecks {

  import Shrink.shrinkAny

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  behavior of "PackageService.buildMap"

  it should "identifiers with the same (moduleName, entityName) are not unique" in
    forAll(genDuplicateApiIdentifiers) { ids =>
      val map = PackageService.buildTemplateIdMap(ids.toSet)
      map.all shouldBe expectedAll(ids)
      map.unique shouldBe Map.empty
    }

  it should "2 identifiers with the same (moduleName, entityName) are not unique" in
    forAll(genApiIdentifier) { id0 =>
      val id1 = id0.copy(packageId = id0.packageId + "aaaa")
      val map = PackageService.buildTemplateIdMap(Set(id0, id1))
      map.all shouldBe expectedAll(List(id0, id1))
      map.unique shouldBe Map.empty
    }

  it should "pass one specific test case that was failing" in {
    val id0 = Identifier("a", "", "f4", "x")
    val id1 = Identifier("b", "", "f4", "x")
    val map = PackageService.buildTemplateIdMap(Set(id0, id1))
    map.all shouldBe expectedAll(List(id0, id1))
    map.unique shouldBe Map.empty
  }

  it should "3 identifiers with the same (moduleName, entityName) are not unique" in
    forAll(genApiIdentifier) { id0 =>
      val id1 = id0.copy(packageId = id0.packageId + "aaaa")
      val id2 = id0.copy(packageId = id0.packageId + "bbbb")
      val map = PackageService.buildTemplateIdMap(Set(id0, id1, id2))
      map.all shouldBe expectedAll(List(id0, id1, id2))
      map.unique shouldBe Map.empty
    }

  it should "all should contain dups and unique identifiers" in
    forAll(nonEmptyListOf(Generators.genApiIdentifier), genDuplicateApiIdentifiers) { (xs, dups) =>
      val map = PackageService.buildTemplateIdMap((xs ++ dups).toSet)
      map.all.size should be >= dups.size
      map.all.size should be >= map.unique.size
      dups.foreach { x =>
        map.all.get(PackageService.key3(x)) shouldBe Some(x)
      }
      xs.foreach { x =>
        map.all.get(PackageService.key3(x)) shouldBe Some(x)
      }
    }

  behavior of "PackageService.resolveTemplateIds"

  it should "return all API Identifier by moduleName and entityName" in forAll(
    nonEmptySet(genApiIdentifier)) { ids =>
    val map = PackageService.buildTemplateIdMap(ids)
    val uniqueIds: Set[Identifier] = map.unique.values.toSet
    val uniqueDomainIds: Set[domain.TemplateId.OptionalPkg] = uniqueIds.map(x =>
      domain.TemplateId(packageId = None, moduleName = x.moduleName, entityName = x.entityName))

    inside(PackageService.resolveTemplateIds(map)(uniqueDomainIds)) {
      case \/-(actualIds) => actualIds.toSet shouldBe uniqueIds
    }
  }

  it should "return all API Identifier by packageId, moduleName and entityName" in forAll(
    nonEmptySet(genApiIdentifier)) { ids =>
    val map = PackageService.buildTemplateIdMap(ids)
    val domainIds: Set[domain.TemplateId.OptionalPkg] =
      ids.map(x => domain.TemplateId(Some(x.packageId), x.moduleName, x.entityName))

    inside(PackageService.resolveTemplateIds(map)(domainIds)) {
      case \/-(actualIds) => actualIds.toSet shouldBe ids
    }
  }

  it should "return error for unmapped ID requested by moduleName and entityName" in forAll(
    nonEmptyListOf(genApiIdentifier),
    nonEmptyListOf(genApiIdentifier)) { (knownIds, otherIds) =>
    val map = PackageService.buildTemplateIdMap(knownIds.toSet)

    val unknownIds: Set[Identifier] = otherIds.toSet diff knownIds.toSet
    val unknownDomainIds: Set[domain.TemplateId.OptionalPkg] = unknownIds.map(x =>
      domain.TemplateId(packageId = None, moduleName = x.moduleName, entityName = x.entityName))

    unknownDomainIds.foreach { id =>
      inside(PackageService.resolveTemplateId(map)(id)) {
        case -\/(e) =>
          val t = domain.TemplateId[Unit]((), id.moduleName, id.entityName)
          e shouldBe s"Cannot resolve ${t.toString}"
      }
    }

    inside(PackageService.resolveTemplateIds(map)(unknownDomainIds)) {
      case -\/(e) => e should startWith("Cannot resolve ")
    }
  }

  it should "return error for unmapped ID requested by entityName, moduleName and entityName" in forAll(
    nonEmptyListOf(genApiIdentifier),
    nonEmptyListOf(genApiIdentifier)) { (knownIds, otherIds) =>
    val map = PackageService.buildTemplateIdMap(knownIds.toSet)

    val unknownIds: Set[Identifier] = otherIds.toSet diff knownIds.toSet
    val unknownDomainIds: Set[domain.TemplateId.OptionalPkg] = unknownIds.map(
      x =>
        domain.TemplateId(
          packageId = Some(x.packageId),
          moduleName = x.moduleName,
          entityName = x.entityName))

    unknownDomainIds.foreach { id =>
      inside(PackageService.resolveTemplateId(map)(id)) {
        case -\/(e) =>
          val t = domain.TemplateId(
            id.packageId.getOrElse(fail("should never happen!")),
            id.moduleName,
            id.entityName)
          e shouldBe s"Cannot resolve ${t.toString}"
      }
    }

    inside(PackageService.resolveTemplateIds(map)(unknownDomainIds)) {
      case -\/(e) => e should startWith("Cannot resolve ")
    }
  }

  private def expectedAll(ids: List[Identifier]): Map[domain.TemplateId.RequiredPkg, Identifier] =
    ids.map(v => PackageService.key3(v) -> v)(breakOut)
}
