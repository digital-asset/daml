// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.http.Generators.{
  genDomainTemplateId,
  genDuplicateModuleEntityTemplateIds,
  nonEmptySetOf,
}
import com.daml.http.PackageService.TemplateIdMap
import com.daml.ledger.api.{v1 => lav1}
import org.scalacheck.Shrink
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class PackageServiceTest
    extends AnyFreeSpec
    with Matchers
    with Inside
    with ScalaCheckDrivenPropertyChecks {

  import Shrink.shrinkAny

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  "PackageService.buildTemplateIdMap" - {
    "identifiers with the same (moduleName, entityName) are not unique" in
      forAll(genDuplicateModuleEntityTemplateIds) { ids =>
        toNoPkgSet(ids) should have size 1L
        val map = PackageService.buildTemplateIdMap(ids)
        map.all.keySet shouldBe ids
        map.all.values should contain theSameElementsAs ids
        map.unique shouldBe Map.empty
      }

    "2 identifiers with the same (moduleName, entityName) are not unique" in
      forAll(genDomainTemplateId) { id0 =>
        val id1 = appendToPackageId("aaaa")(id0)
        val map = PackageService.buildTemplateIdMap(Set(id0, id1))
        map.all.keySet shouldBe Set(id0, id1)
        map.unique shouldBe Map.empty
      }

    "pass one specific test case that was failing" in {
      val id0 = domain.ContractTypeId.Template.fromLedgerApi(lav1.value.Identifier("a", "f4", "x"))
      val id1 = domain.ContractTypeId.Template.fromLedgerApi(lav1.value.Identifier("b", "f4", "x"))
      val map = PackageService.buildTemplateIdMap(Set(id0, id1))
      map.all.keySet shouldBe Set(id0, id1)
      map.unique shouldBe Map.empty
    }

    "3 identifiers with the same (moduleName, entityName) are not unique" in
      forAll(genDomainTemplateId) { id0 =>
        val id1 = appendToPackageId("aaaa")(id0)
        val id2 = appendToPackageId("bbbb")(id1)
        val map = PackageService.buildTemplateIdMap(Set(id0, id1, id2))
        map.all.keySet shouldBe Set(id0, id1, id2)
        map.unique shouldBe Map.empty
      }

    "TemplateIdMap.all should contain dups and unique identifiers" in
      forAll(
        nonEmptySetOf(genDomainTemplateId),
        genDuplicateModuleEntityTemplateIds,
      ) { (xs, dups) =>
        uniqueModuleEntity(dups) shouldBe false
        whenever(uniqueModuleEntity(xs) && noModuleEntityIntersection(xs, dups)) {
          val map = PackageService.buildTemplateIdMap((xs ++ dups))
          map.all.keySet should ===(xs ++ dups)
          map.all.keySet should contain allElementsOf dups
          map.all.keySet should contain allElementsOf xs
        }
      }

    "TemplateIdMap.unique should not contain dups" in
      forAll(nonEmptySetOf(genDomainTemplateId), genDuplicateModuleEntityTemplateIds) {
        (xs, dups) =>
          uniqueModuleEntity(dups) shouldBe false
          whenever(uniqueModuleEntity(xs) && noModuleEntityIntersection(xs, dups)) {
            val map = PackageService.buildTemplateIdMap((xs ++ dups))
            map.all.keySet should ===(dups ++ xs)
            xs.foreach { x =>
              map.unique.get(PackageService.key2(x)) shouldBe Some(x)
            }
            dups.foreach { x =>
              map.unique.get(PackageService.key2(x)) shouldBe None
            }
          }
      }
  }

  "PackageService.resolveTemplateId" - {

    "should resolve unique Template ID by (moduleName, entityName)" in forAll(
      nonEmptySetOf(genDomainTemplateId)
    ) { ids =>
      val map = PackageService.buildTemplateIdMap(ids)
      val uniqueIds = map.unique.values.toSet
      uniqueIds.foreach { id =>
        val unresolvedId: domain.ContractTypeId.Template.OptionalPkg = id.copy(packageId = None)
        map resolve unresolvedId shouldBe Some(id)
      }
    }

    "should resolve fully qualified Template ID" in forAll(nonEmptySetOf(genDomainTemplateId)) {
      ids =>
        val map = PackageService.buildTemplateIdMap(ids)
        ids.foreach { id =>
          val unresolvedId: domain.ContractTypeId.Template.OptionalPkg =
            id.copy(packageId = Some(id.packageId))
          map resolve unresolvedId shouldBe Some(id)
        }
    }

    "should return None for unknown Template ID" in forAll(
      Generators.genDomainTemplateIdO: org.scalacheck.Gen[domain.ContractTypeId.OptionalPkg]
    ) { templateId: domain.ContractTypeId.OptionalPkg =>
      val map = TemplateIdMap.Empty[domain.ContractTypeId]
      map resolve templateId shouldBe None
    }
  }

  private def appendToPackageId[
      CtId[T] <: domain.ContractTypeId[T] with domain.ContractTypeId.Ops[CtId, T]
  ](x: String)(a: CtId[String]) =
    a.copy(packageId = a.packageId + x)

  private def uniqueModuleEntity(as: Set[_ <: domain.ContractTypeId.RequiredPkg]): Boolean =
    toNoPkgSet[domain.ContractTypeId](as).size == as.size

  private def noModuleEntityIntersection[CtId[T] <: domain.ContractTypeId.Ops[CtId, T]](
      as: Set[CtId[String]],
      bs: Set[CtId[String]],
  ): Boolean =
    !(toNoPkgSet(as) exists toNoPkgSet(bs))

  private def toNoPkgSet[CtId[T] <: domain.ContractTypeId.Ops[CtId, T]](
      xs: Set[_ <: CtId[String]]
  ): Set[CtId[Unit]] =
    xs.map(_ copy (packageId = ()))
}
