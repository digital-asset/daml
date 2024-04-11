// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.http.Generators.{
  genDomainTemplateId,
  genDuplicateModuleEntityTemplateIds,
  nonEmptySetOf,
}
import com.daml.http.PackageService.TemplateIdMap
import com.daml.ledger.api.{v1 => lav1}
import com.daml.lf.data.Ref
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
        val map = PackageService.buildTemplateIdMap(noPackageNames, ids)
        map.all.keySet shouldBe ids
        map.all.values should contain theSameElementsAs ids
        map.unique shouldBe Map.empty
      }

    "2 identifiers with the same (moduleName, entityName) are not unique" in
      forAll(genDomainTemplateId) { id0 =>
        val id1 = appendToPackageId("aaaa")(id0)
        val map = PackageService.buildTemplateIdMap(noPackageNames, Set(id0, id1))
        map.all.keySet shouldBe Set(id0, id1)
        map.unique shouldBe Map.empty
      }

    "pass one specific test case that was failing" in {
      val id0 = domain.ContractTypeId.Template.fromLedgerApi(lav1.value.Identifier("a", "f4", "x"))
      val id1 = domain.ContractTypeId.Template.fromLedgerApi(lav1.value.Identifier("b", "f4", "x"))
      val map = PackageService.buildTemplateIdMap(noPackageNames, Set(id0, id1))
      map.all.keySet shouldBe Set(id0, id1)
      map.unique shouldBe Map.empty
    }

    "3 identifiers with the same (moduleName, entityName) are not unique" in
      forAll(genDomainTemplateId) { id0 =>
        val id1 = appendToPackageId("aaaa")(id0)
        val id2 = appendToPackageId("bbbb")(id1)
        val map = PackageService.buildTemplateIdMap(noPackageNames, Set(id0, id1, id2))
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
          val map = PackageService.buildTemplateIdMap(noPackageNames, (xs ++ dups))
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
            val map = PackageService.buildTemplateIdMap(noPackageNames, (xs ++ dups))
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
      val map = PackageService.buildTemplateIdMap(noPackageNames, ids)
      val uniqueIds = map.unique.values.toSet
      uniqueIds.foreach { id =>
        val unresolvedId: domain.ContractTypeId.Template.OptionalPkg = id.copy(packageId = None)
        map resolve unresolvedId shouldBe Some(id)
      }
    }

    "should resolve fully qualified Template ID" in forAll(nonEmptySetOf(genDomainTemplateId)) {
      ids =>
        val map = PackageService.buildTemplateIdMap(noPackageNames, ids)
        ids.foreach { id =>
          val unresolvedId: domain.ContractTypeId.Template.OptionalPkg =
            id.copy(packageId = Some(id.packageId))
          map resolve unresolvedId shouldBe Some(id)
        }
    }

    "should resolve by package name when single package id" in forAll(
      nonEmptySetOf(genDomainTemplateId)
    ) { ids =>
      def pkgNameForPkgId(pkgId: String) = pkgId + "_name"
      val idName = buildPackageNameMap(pkgNameForPkgId)(ids) // package_id:package_name is 1:1
      val map = PackageService.buildTemplateIdMap(idName, ids)
      ids.foreach { id =>
        val pkgName = "#" + pkgNameForPkgId(id.packageId)
        map resolve id.copy(packageId = Some(pkgName)) shouldBe Some(id.copy(packageId = pkgName))
      }
    }

    "should resolve by package name when multiple package ids" in forAll(
      genDuplicateModuleEntityTemplateIds
    ) { ids =>
      val idName = buildPackageNameMap(_ => "foo")(ids) // package_id:package_name is n:1
      val pkgIdWithMaxVer = ids.maxBy(id => packageVersionForId(id.packageId))
      val map = PackageService.buildTemplateIdMap(idName, ids)
      ids.foreach { id =>
        val templateIdWithPackageName = id.copy(packageId = Some("#foo"))
        val Some(resolvedTemplateId) = map resolve templateIdWithPackageName

        // Selects a package id with the given package name.
        idName(resolvedTemplateId.packageId)._1.toOption shouldBe Some(
          Ref.PackageName.assertFromString("foo")
        )
        // Should have selected package with highest version
        resolvedTemplateId.packageId == pkgIdWithMaxVer
        // The module and entity part of the template ids should be the same.
        resolvedTemplateId.copy(packageId = None) shouldBe id.copy(packageId = None)
      }
    }

    "should return None for unknown Template ID" in forAll(
      Generators.genDomainTemplateIdO: org.scalacheck.Gen[domain.ContractTypeId.OptionalPkg]
    ) { templateId: domain.ContractTypeId.OptionalPkg =>
      val map = TemplateIdMap.Empty[domain.ContractTypeId]
      map resolve templateId shouldBe None
    }
  }

  // Arbitrary but deterministic assignment of package version to package id.
  private def packageVersionForId(pkgId: String) = {
    Ref.PackageVersion.assertFromString(s"0.0.${pkgId.hashCode.abs}")
  }

  private def buildPackageNameMap(
      pkgNameForPkgId: (String => String)
  )(ids: Set[_ <: domain.ContractTypeId.RequiredPkg]): PackageService.PackageNameMap = {
    import com.daml.lf.crypto.Hash.KeyPackageName
    import com.daml.lf.language.LanguageVersion
    PackageService.PackageNameMap(
      ids
        .flatMap((id: domain.ContractTypeId.RequiredPkg) => {
          val pkgName = pkgNameForPkgId(id.packageId)
          val pkgVersion = packageVersionForId(id.packageId)
          val kpn =
            KeyPackageName(Some(Ref.PackageName.assertFromString(pkgName)), LanguageVersion.v1_dev)
          Set(
            (id.packageId, (kpn, pkgVersion)),
            ("#" + pkgName, (kpn, pkgVersion)),
          )
        })
        .toMap
        .view
    )
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

  private val noPackageNames: PackageService.PackageNameMap = PackageService.PackageNameMap.empty
}
