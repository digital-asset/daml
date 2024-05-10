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

    "pass one specific test case that was failing" in {
      val id0 = domain.ContractTypeId.Template.fromLedgerApi(lav1.value.Identifier("a", "f4", "x"))
      val id1 = domain.ContractTypeId.Template.fromLedgerApi(lav1.value.Identifier("b", "f4", "x"))
      val map = PackageService.buildTemplateIdMap(noPackageNames, Set(id0, id1))
      map.all.keySet shouldBe Set(id0, id1)
    }

    "TemplateIdMap.all should contain dups and unique identifiers" in
      forAll(
        nonEmptySetOf(genDomainTemplateId),
        genDuplicateModuleEntityTemplateIds,
      ) { (xs, dups) =>
        val map = PackageService.buildTemplateIdMap(noPackageNames, (xs ++ dups))
        map.all.keySet should ===(xs ++ dups)
        map.all.keySet should contain allElementsOf dups
        map.all.keySet should contain allElementsOf xs
      }
  }

  "PackageService.resolveContractTypeId" - {

    "should resolve fully qualified Template ID" in forAll(nonEmptySetOf(genDomainTemplateId)) {
      ids =>
        val map = PackageService.buildTemplateIdMap(noPackageNames, ids)
        ids.foreach { id =>
          val unresolvedId: domain.ContractTypeId.Template.RequiredPkg = id
          val Some(resolved) = map resolve unresolvedId
          resolved.original shouldBe id
          resolved.latestPkgId shouldBe id
          resolved.allPkgIds shouldBe Set(id)
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
        val unresolvedId: domain.ContractTypeId.Template.RequiredPkg = id.copy(packageId = pkgName)
        val Some(resolved) = map resolve unresolvedId
        resolved.original shouldBe id.copy(packageId = pkgName)
        resolved.latestPkgId shouldBe id
        resolved.allPkgIds shouldBe Set(id)
      }
    }

    "should resolve by package name when multiple package ids" in forAll(
      genDuplicateModuleEntityTemplateIds
    ) { ids =>
      val idName = buildPackageNameMap(_ => "foo")(ids) // package_id:package_name is n:1
      val idWithMaxVer = ids.maxBy(id => packageVersionForId(id.packageId))
      val map = PackageService.buildTemplateIdMap(idName, ids)
      ids.foreach { id =>
        val unresolvedId: domain.ContractTypeId.Template.RequiredPkg = id.copy(packageId = "#foo")
        val Some(resolved) = map resolve unresolvedId
        resolved.original shouldBe id.copy(packageId = "#foo")
        resolved.latestPkgId shouldBe idWithMaxVer
        resolved.allPkgIds shouldBe ids
      }
    }

    "should return None for unknown Template ID" in forAll(
      Generators.genDomainTemplateIdO: org.scalacheck.Gen[domain.ContractTypeId.RequiredPkg]
    ) { templateId: domain.ContractTypeId.RequiredPkg =>
      val map = TemplateIdMap.Empty[domain.ContractTypeId]
      map resolve templateId shouldBe None
    }
  }

  "PackageService.allTemplateIds" - {
    "when no package names, should resolve to input ids" in forAll(
      nonEmptySetOf(genDomainTemplateId)
    ) { ids =>
      val map = PackageService.buildTemplateIdMap(noPackageNames, ids)
      map.allIds.size shouldBe ids.size
      map.allIds.map(_.original) shouldBe ids
      map.allIds.map(_.latestPkgId) shouldBe ids
      map.allIds.flatMap(_.allPkgIds) shouldBe ids
    }

    "when has single package name per package id, each has has its own item" in forAll(
      nonEmptySetOf(genDomainTemplateId)
    ) { ids =>
      def pkgNameForPkgId(pkgId: String) = pkgId + "_name"
      val idName = buildPackageNameMap(pkgNameForPkgId)(ids) // package_id:package_name is 1:1
      val map = PackageService.buildTemplateIdMap(idName, ids)

      map.allIds.size shouldBe ids.size
      map.allIds.map(_.original) shouldBe ids.map { id =>
        id.copy(packageId = s"#${pkgNameForPkgId(id.packageId)}")
      }
      map.allIds.map(_.latestPkgId) shouldBe ids
      map.allIds.map(_.allPkgIds) shouldBe ids.map(Set(_))
    }

    "when has multiple names per package id, they are collapsed into a single item" in forAll(
      genDuplicateModuleEntityTemplateIds
    ) { ids =>
      val idName = buildPackageNameMap(_ => "foo")(ids) // package_id:package_name is n:1
      val idWithMaxVer = ids.maxBy(id => packageVersionForId(id.packageId))
      val map = PackageService.buildTemplateIdMap(idName, ids)

      map.allIds.size shouldBe 1
      map.allIds.head.original shouldBe ids.head.copy(packageId = "#foo")
      map.allIds.head.latestPkgId shouldBe idWithMaxVer
      map.allIds.head.allPkgIds shouldBe ids
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

  private val noPackageNames: PackageService.PackageNameMap = PackageService.PackageNameMap.empty
}
