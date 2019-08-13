// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.http.Generators.{
  genDomainTemplateId,
  genDuplicateDomainTemplateIdR,
  nonEmptySet
}
import com.digitalasset.http.PackageService.TemplateIdMap
import com.digitalasset.ledger.api.{v1 => lav1}
import org.scalacheck.Gen.nonEmptyListOf
import org.scalacheck.Shrink
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FreeSpec, Inside, Matchers}
import scalaz.{-\/, \/-}

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
      forAll(genDuplicateDomainTemplateIdR) { ids =>
        val map = PackageService.buildTemplateIdMap(ids.toSet)
        map.all shouldBe ids.toSet
        map.unique shouldBe Map.empty
      }

    "2 identifiers with the same (moduleName, entityName) are not unique" in
      forAll(genDomainTemplateId) { id0 =>
        val id1 = appendToPackageId("aaaa")(id0)
        val map = PackageService.buildTemplateIdMap(Set(id0, id1))
        map.all shouldBe Set(id0, id1)
        map.unique shouldBe Map.empty
      }

    "pass one specific test case that was failing" in {
      val id0 = domain.TemplateId.fromLedgerApi(lav1.value.Identifier("a", "f4", "x"))
      val id1 = domain.TemplateId.fromLedgerApi(lav1.value.Identifier("b", "f4", "x"))
      val map = PackageService.buildTemplateIdMap(Set(id0, id1))
      map.all shouldBe Set(id0, id1)
      map.unique shouldBe Map.empty
    }

    "3 identifiers with the same (moduleName, entityName) are not unique" in
      forAll(genDomainTemplateId) { id0 =>
        val id1 = appendToPackageId("aaaa")(id0)
        val id2 = appendToPackageId("bbbb")(id1)
        val map = PackageService.buildTemplateIdMap(Set(id0, id1, id2))
        map.all shouldBe Set(id0, id1, id2)
        map.unique shouldBe Map.empty
      }

    "TemplateIdMap.all should contain dups and unique identifiers" in
      forAll(nonEmptyListOf(genDomainTemplateId), genDuplicateDomainTemplateIdR) { (xs, dups) =>
        val map = PackageService.buildTemplateIdMap((xs ++ dups).toSet)
        map.all should ===(xs.toSet ++ dups.toSet)
        dups.foreach { x =>
          map.all.contains(x) shouldBe true
        }
        xs.foreach { x =>
          map.all.contains(x) shouldBe true
        }
      }

    "TemplateIdMap.unique should not contain dups" in
      forAll(nonEmptyListOf(genDomainTemplateId), genDuplicateDomainTemplateIdR) { (xs, dups) =>
        val map = PackageService.buildTemplateIdMap((xs ++ dups).toSet)
        map.all should ===(dups.toSet ++ xs.toSet)
        xs.foreach { x =>
          map.unique.get(PackageService.key2(x)) shouldBe Some(x)
        }
        dups.foreach { x =>
          map.unique.get(PackageService.key2(x)) shouldBe None
        }
      }
  }

  "PackageService.resolveTemplateIds" - {

    "should return all API Identifier by (moduleName, entityName)" in forAll(
      nonEmptySet(genDomainTemplateId)) { ids =>
      val map = PackageService.buildTemplateIdMap(ids)
      val uniqueIds: Set[domain.TemplateId.RequiredPkg] = map.unique.values.toSet
      val uniqueDomainIds: Set[domain.TemplateId.OptionalPkg] = uniqueIds.map { x =>
        domain.TemplateId(packageId = None, moduleName = x.moduleName, entityName = x.entityName)
      }

      inside(PackageService.resolveTemplateIds(map)(uniqueDomainIds)) {
        case \/-(actualIds) => actualIds.toSet shouldBe uniqueIds
      }
    }

    "should return all API Identifier by (packageId, moduleName, entityName)" in forAll(
      nonEmptySet(genDomainTemplateId)) { ids =>
      val map = PackageService.buildTemplateIdMap(ids)
      val domainIds: Set[domain.TemplateId.OptionalPkg] =
        ids.map { x =>
          domain.TemplateId(Some(x.packageId), x.moduleName, x.entityName)
        }

      inside(PackageService.resolveTemplateIds(map)(domainIds)) {
        case \/-(actualIds) => actualIds.toSet shouldBe ids
      }
    }

    "should return error for unmapped Template ID" in forAll(
      Generators.genDomainTemplateIdO[Option[String]]) {
      templateId: domain.TemplateId.OptionalPkg =>
        val map = TemplateIdMap(Set.empty, Map.empty)
        inside(PackageService.resolveTemplateId(map)(templateId)) {
          case -\/(e) =>
            val templateIdStr: String = templateId.packageId.fold(
              domain.TemplateId((), templateId.moduleName, templateId.entityName).toString)(p =>
              domain.TemplateId(p, templateId.moduleName, templateId.entityName).toString)
            e shouldBe PackageService.InputError(s"Cannot resolve $templateIdStr")
        }
    }
  }

  private def appendToPackageId(x: String)(a: domain.TemplateId.RequiredPkg) =
    a.copy(packageId = a.packageId + x)

}
