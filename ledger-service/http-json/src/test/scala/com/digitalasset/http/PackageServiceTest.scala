// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => lav1}
import com.digitalasset.http.Generators.{genApiTemplateId, genDuplicateApiTemplateId, nonEmptySet}
import com.digitalasset.http.PackageService.TemplateIdMap
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
      forAll(genDuplicateApiTemplateId) { ids =>
        val map = PackageService.buildTemplateIdMap(ids.toSet)
        map.all shouldBe expectedAll(ids)
        map.unique shouldBe Map.empty
      }

    "2 identifiers with the same (moduleName, entityName) are not unique" in
      forAll(genApiTemplateId) { id0 =>
        val id1 = transform(id0)(appendToPackageId("aaaa"))
        val map = PackageService.buildTemplateIdMap(Set(id0, id1))
        map.all shouldBe expectedAll(List(id0, id1))
        map.unique shouldBe Map.empty
      }

    "pass one specific test case that was failing" in {
      val id0 = lar.TemplateId(lav1.value.Identifier("a", "", "f4", "x"))
      val id1 = lar.TemplateId(lav1.value.Identifier("b", "", "f4", "x"))
      val map = PackageService.buildTemplateIdMap(Set(id0, id1))
      map.all shouldBe expectedAll(List(id0, id1))
      map.unique shouldBe Map.empty
    }

    "3 identifiers with the same (moduleName, entityName) are not unique" in
      forAll(genApiTemplateId) { id0 =>
        val id1 = transform(id0)(appendToPackageId("aaaa"))
        val id2 = transform(id0)(appendToPackageId("bbbb"))
        val map = PackageService.buildTemplateIdMap(Set(id0, id1, id2))
        map.all shouldBe expectedAll(List(id0, id1, id2))
        map.unique shouldBe Map.empty
      }

    "TemplateIdMap.all should contain dups and unique identifiers" in
      forAll(nonEmptyListOf(genApiTemplateId), genDuplicateApiTemplateId) { (xs, dups) =>
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
      forAll(nonEmptyListOf(genApiTemplateId), genDuplicateApiTemplateId) { (xs, dups) =>
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
      nonEmptySet(genApiTemplateId)) { ids =>
      val map = PackageService.buildTemplateIdMap(ids)
      val uniqueIds: Set[lar.TemplateId] = map.unique.values.toSet
      val uniqueDomainIds: Set[domain.TemplateId.OptionalPkg] = uniqueIds.map { x =>
        val y: lav1.value.Identifier = lar.TemplateId.unwrap(x)
        domain.TemplateId(packageId = None, moduleName = y.moduleName, entityName = y.entityName)
      }

      inside(PackageService.resolveTemplateIds(map)(uniqueDomainIds)) {
        case \/-(actualIds) => actualIds.toSet shouldBe uniqueIds
      }
    }

    "should return all API Identifier by (packageId, moduleName, entityName)" in forAll(
      nonEmptySet(genApiTemplateId)) { ids =>
      val map = PackageService.buildTemplateIdMap(ids)
      val domainIds: Set[domain.TemplateId.OptionalPkg] =
        ids.map { x =>
          val y: lav1.value.Identifier = lar.TemplateId.unwrap(x)
          domain.TemplateId(Some(y.packageId), y.moduleName, y.entityName)
        }

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
          e shouldBe PackageService.InputError(s"Cannot resolve $templateIdStr")
      }
    }
  }

  private def expectedAll(
      ids: List[lar.TemplateId]): Map[domain.TemplateId.RequiredPkg, lar.TemplateId] =
    ids.map(v => PackageService.key3(v) -> v)(breakOut)

  private def transform(a: lar.TemplateId)(
      f: lav1.value.Identifier => lav1.value.Identifier): lar.TemplateId = {
    val b = lar.TemplateId.unwrap(a)
    lar.TemplateId(f(b))
  }

  private def appendToPackageId(x: String)(a: lav1.value.Identifier): lav1.value.Identifier =
    a.copy(packageId = a.packageId + x)

}
