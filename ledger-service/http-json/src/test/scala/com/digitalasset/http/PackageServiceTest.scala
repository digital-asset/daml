// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.http.Generators.{genApiIdentifier, genDuplicateApiIdentifiers, nonEmptySet}
import com.digitalasset.http.PackageService.TemplateIdMap
import com.digitalasset.ledger.api.v1.value.Identifier
import org.scalacheck.Gen.nonEmptyListOf
import org.scalacheck.Shrink
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FreeSpec, Inside, Matchers}
import scalaz.{-\/, \/-}

import scala.collection.breakOut

class PackageServiceTest
    extends FreeSpec
    with Matchers
    with Inside
    with GeneratorDrivenPropertyChecks {

  import Shrink.shrinkAny

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  "PackageService.buildTemplateIdMap" - {
    "identifiers with the same (moduleName, entityName) are not unique" in
      forAll(genDuplicateApiIdentifiers) { ids =>
        val map = PackageService.buildTemplateIdMap(ids.toSet)
        map.all shouldBe expectedAll(ids)
        map.unique shouldBe Map.empty
      }

    "2 identifiers with the same (moduleName, entityName) are not unique" in
      forAll(genApiIdentifier) { id0 =>
        val id1 = id0.copy(packageId = id0.packageId + "aaaa")
        val map = PackageService.buildTemplateIdMap(Set(id0, id1))
        map.all shouldBe expectedAll(List(id0, id1))
        map.unique shouldBe Map.empty
      }

    "pass one specific test case that was failing" in {
      val id0 = Identifier("a", "", "f4", "x")
      val id1 = Identifier("b", "", "f4", "x")
      val map = PackageService.buildTemplateIdMap(Set(id0, id1))
      map.all shouldBe expectedAll(List(id0, id1))
      map.unique shouldBe Map.empty
    }

    "3 identifiers with the same (moduleName, entityName) are not unique" in
      forAll(genApiIdentifier) { id0 =>
        val id1 = id0.copy(packageId = id0.packageId + "aaaa")
        val id2 = id0.copy(packageId = id0.packageId + "bbbb")
        val map = PackageService.buildTemplateIdMap(Set(id0, id1, id2))
        map.all shouldBe expectedAll(List(id0, id1, id2))
        map.unique shouldBe Map.empty
      }

    "TemplateIdMap.all should contain dups and unique identifiers" in
      forAll(nonEmptyListOf(Generators.genApiIdentifier), genDuplicateApiIdentifiers) {
        (xs, dups) =>
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

    "TemplateIdMap.unique should not contain dups" in
      forAll(nonEmptyListOf(Generators.genApiIdentifier), genDuplicateApiIdentifiers) {
        (xs, dups) =>
          val map = PackageService.buildTemplateIdMap((xs ++ dups).toSet)
          map.all.size should be >= dups.size
          map.all.size should be >= map.unique.size
          xs.foreach { x =>
            map.unique.get(PackageService.key2(PackageService.key3(x))) shouldBe Some(x)
          }
          dups.foreach { x =>
            map.unique.get(PackageService.key2(PackageService.key3(x))) shouldBe None
          }
      }
  }

  "PackageService.resolveTemplateIds" - {

    "should return all API Identifier by (moduleName, entityName)" in forAll(
      nonEmptySet(genApiIdentifier)) { ids =>
      val map = PackageService.buildTemplateIdMap(ids)
      val uniqueIds: Set[Identifier] = map.unique.values.toSet
      val uniqueDomainIds: Set[domain.TemplateId.OptionalPkg] = uniqueIds.map(x =>
        domain.TemplateId(packageId = None, moduleName = x.moduleName, entityName = x.entityName))

      inside(PackageService.resolveTemplateIds(map)(uniqueDomainIds)) {
        case \/-(actualIds) => actualIds.toSet shouldBe uniqueIds
      }
    }

    "should return all API Identifier by (packageId, moduleName, entityName)" in forAll(
      nonEmptySet(genApiIdentifier)) { ids =>
      val map = PackageService.buildTemplateIdMap(ids)
      val domainIds: Set[domain.TemplateId.OptionalPkg] =
        ids.map(x => domain.TemplateId(Some(x.packageId), x.moduleName, x.entityName))

      inside(PackageService.resolveTemplateIds(map)(domainIds)) {
        case \/-(actualIds) => actualIds.toSet shouldBe ids
      }
    }

    "should return error for unmapped Template ID" in forAll(
      Generators.genTemplateId[Option[String]]) { templateId: domain.TemplateId.OptionalPkg =>
      val map = TemplateIdMap(Map.empty, Map.empty)
      inside(PackageService.resolveTemplateId(map)(templateId)) {
        case -\/(e) =>
          val templateIdStr: String = templateId.packageId.fold(
            domain.TemplateId((), templateId.moduleName, templateId.entityName).toString)(p =>
            domain.TemplateId(p, templateId.moduleName, templateId.entityName).toString)
          e shouldBe s"Cannot resolve $templateIdStr"
      }
    }
  }

  private def expectedAll(ids: List[Identifier]): Map[domain.TemplateId.RequiredPkg, Identifier] =
    ids.map(v => PackageService.key3(v) -> v)(breakOut)
}
