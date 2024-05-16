// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.http.Generators.{
  genDomainTemplateIdPkgId,
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
  import domain.ContractTypeId.withPkgRef

  def pkgRefName(s: String): Ref.PackageRef =
    Ref.PackageRef.Name(Ref.PackageName.assertFromString(s))

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  "PackageService.buildTemplateIdMap" - {

    "pass one specific test case that was failing" in {
      val id0 = domain.ContractTypeId.Template.fromLedgerApi(lav1.value.Identifier("a", "f4", "x"))
      val id1 = domain.ContractTypeId.Template.fromLedgerApi(lav1.value.Identifier("b", "f4", "x"))
      val map = PackageService.buildTemplateIdMap(noPackageNames, Set(id0, id1))
      map.all.keySet shouldBe Set(id0, id1).map(withPkgRef)
    }

    "TemplateIdMap.all should contain dups and unique identifiers" in
      forAll(
        nonEmptySetOf(genDomainTemplateIdPkgId),
        genDuplicateModuleEntityTemplateIds,
      ) { (xs, dups) =>
        val map = PackageService.buildTemplateIdMap(noPackageNames, (xs ++ dups))
        map.all.keySet should ===((xs ++ dups).map(withPkgRef))
        map.all.keySet should contain allElementsOf dups.map(withPkgRef)
        map.all.keySet should contain allElementsOf xs.map(withPkgRef)
      }
  }

  "PackageService.resolveContractTypeId" - {

    "should resolve fully qualified Template ID" in forAll(
      nonEmptySetOf(genDomainTemplateIdPkgId)
    ) { ids =>
      val map = PackageService.buildTemplateIdMap(noPackageNames, ids)
      ids.foreach { id =>
        val unresolvedId: domain.ContractTypeId.Template.RequiredPkg = withPkgRef(id)
        val Some(resolved) = map resolve unresolvedId
        resolved.original shouldBe withPkgRef(id)
        resolved.latestPkgId shouldBe id
        resolved.allPkgIds shouldBe Set(id)
      }
    }

    "should resolve by package name when single package id" in forAll(
      nonEmptySetOf(genDomainTemplateIdPkgId)
    ) { ids =>
      def pkgNameForPkgId(pkgId: String) = pkgId + "_name"
      val idName = buildPackageNameMap(pkgNameForPkgId)(ids) // package_id:package_name is 1:1
      val map = PackageService.buildTemplateIdMap(idName, ids)
      ids.foreach { id =>
        val pkgName = pkgRefName(pkgNameForPkgId(id.packageId))
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
        val unresolvedId: domain.ContractTypeId.Template.RequiredPkg =
          id.copy(packageId = pkgRefName("foo"))
        val Some(resolved) = map resolve unresolvedId
        resolved.original shouldBe id.copy(packageId = pkgRefName("foo"))
        resolved.latestPkgId shouldBe idWithMaxVer
        resolved.allPkgIds shouldBe ids
      }
    }

    "should return None for unknown Template ID" in forAll(
      Generators.genDomainTemplateIdO: org.scalacheck.Gen[domain.ContractTypeId.RequiredPkg]
    ) { templateId: domain.ContractTypeId.RequiredPkg =>
      val map = TemplateIdMap.Empty[domain.ContractTypeId.Template]
      map resolve templateId shouldBe None
    }
  }

  "PackageService.allTemplateIds" - {
    "when no package names, should resolve to input ids" in forAll(
      nonEmptySetOf(genDomainTemplateIdPkgId)
    ) { ids =>
      val map = PackageService.buildTemplateIdMap(noPackageNames, ids)
      map.allIds.size shouldBe ids.size
      map.allIds.map(_.original) shouldBe ids.map(withPkgRef)
      map.allIds.map(_.latestPkgId) shouldBe ids
      map.allIds.flatMap(_.allPkgIds) shouldBe ids
    }

    "when has single package name per package id, each has has its own item" in forAll(
      nonEmptySetOf(genDomainTemplateIdPkgId)
    ) { ids =>
      def pkgNameForPkgId(pkgId: String) = pkgId + "_name"
      val idName = buildPackageNameMap(pkgNameForPkgId)(ids) // package_id:package_name is 1:1
      val map = PackageService.buildTemplateIdMap(idName, ids)

      map.allIds.size shouldBe ids.size
      map.allIds.map(_.original) shouldBe ids.map { id =>
        id.copy(packageId = pkgRefName(pkgNameForPkgId(id.packageId)))
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
      map.allIds.head.original shouldBe ids.head.copy(packageId = pkgRefName("foo"))
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
  )(ids: Set[_ <: domain.ContractTypeId.RequiredPkgId]): PackageService.PackageNameMap = {
    import com.daml.lf.crypto.Hash.KeyPackageName
    import com.daml.lf.language.LanguageVersion
    PackageService.PackageNameMap(
      ids
        .flatMap((id: domain.ContractTypeId.RequiredPkgId) => {
          val pkgName = Ref.PackageName.assertFromString(pkgNameForPkgId(id.packageId.toString))
          val pkgVersion = packageVersionForId(id.packageId)
          val kpn =
            KeyPackageName(Some(pkgName), LanguageVersion.v1_dev)
          Set(
            (Ref.PackageRef.Id(id.packageId), (kpn, pkgVersion)),
            (Ref.PackageRef.Name(pkgName), (kpn, pkgVersion)),
          )
        })
        .toMap
        .view
    )
  }

  private val noPackageNames: PackageService.PackageNameMap = PackageService.PackageNameMap.empty
}
