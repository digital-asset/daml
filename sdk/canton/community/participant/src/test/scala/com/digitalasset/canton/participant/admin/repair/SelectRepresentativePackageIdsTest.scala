// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.Counter
import com.digitalasset.canton.participant.admin.data.{
  ContractImportMode,
  RepairContract,
  RepresentativePackageIdOverride,
}
import com.digitalasset.canton.protocol.{ExampleTransactionFactory, LfContractId}
import com.digitalasset.canton.store.packagemeta.PackageMetadata.{
  LocalPackagePreference,
  PackageResolution,
}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{BaseTest, LfPackageId, LfPackageName, LfPackageVersion}
import com.digitalasset.daml.lf.data.{Bytes, Ref, Time}
import com.digitalasset.daml.lf.transaction.test.TestNodeBuilder
import com.digitalasset.daml.lf.transaction.{CreationTime, FatContractInstance}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ValueUnit
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

private[repair] class SelectRepresentativePackageIdsTest extends AnyWordSpec with BaseTest {

  classOf[SelectRepresentativePackageIds].getSimpleName when {
    "selecting representative package IDs" should {
      "use the highest versioned local package for the contract's package name when only that package is available" in {
        import TestValues.*
        testPrecedence(
          knownPackages = Set(highestVersionedLocalPackage),
          expectation = Right(highestVersionedLocalPackage),
        )
      }

      "use the package-name override as higher precedence than the highest versioned local package" in {
        import TestValues.*
        testPrecedence(
          knownPackages = Set(highestVersionedLocalPackage, packageNameOverride),
          expectation = Right(packageNameOverride),
        )
      }

      "use the contract's creation package-id as higher precedence than the package-name override" in {
        import TestValues.*
        testPrecedence(
          knownPackages = Set(
            highestVersionedLocalPackage,
            packageNameOverride,
            creationPkgId,
          ),
          expectation = Right(creationPkgId),
        )
      }

      "use the exported contract's representative package-id as higher precedence than the contract's creation package-id" in {
        import TestValues.*
        testPrecedence(
          knownPackages = Set(
            highestVersionedLocalPackage,
            packageNameOverride,
            creationPkgId,
            rpId,
          ),
          expectation = Right(rpId),
        )
      }

      "use the package-id override for the contract's creation package-id as higher precedence than the exported contract's representative package-id" in {
        import TestValues.*
        testPrecedence(
          knownPackages = Set(
            highestVersionedLocalPackage,
            packageNameOverride,
            creationPkgId,
            rpId,
            creationPackageIdOverride,
          ),
          expectation = Right(creationPackageIdOverride),
        )
      }

      "use the package-id override for the exported contract's representative package-id as higher precedence than the package-id override for the contract's creation package-id" in {
        import TestValues.*
        testPrecedence(
          knownPackages = Set(
            highestVersionedLocalPackage,
            packageNameOverride,
            creationPkgId,
            rpId,
            creationPackageIdOverride,
            rpIdPackageIdOverride,
          ),
          expectation = Right(rpIdPackageIdOverride),
        )
      }

      "use the contract-id override as highest precedence" in {
        import TestValues.*
        testPrecedence(
          knownPackages = Set(
            highestVersionedLocalPackage,
            packageNameOverride,
            creationPkgId,
            rpId,
            creationPackageIdOverride,
            contractRpIdOverride,
          ),
          expectation = Right(contractRpIdOverride),
        )
      }
    }

    "fail if no candidate representative package-id is known" in {
      import TestValues.*

      testPrecedence(
        knownPackages = Set(LfPackageId.assertFromString("unknown-package-id")),
        expectation = Left(
          s"Could not select a representative package-id for contract with id $contractId. No package in store for the contract's package-name 'somePkgName'."
        ),
      )
    }

    s"fail if the selected package-id differs from the exported representative package-id in ${ContractImportMode.Accept}" in {
      import TestValues.*

      testPrecedence(
        knownPackages = Set(contractRpIdOverride),
        expectation = Left(
          show"Contract import mode is 'Accept' but the selected representative package-id $contractRpIdOverride " +
            show"for contract with id ${repairContract.contract.contractId} differs from the exported representative package-id ${repairContract.representativePackageId}. " +
            show"Please use contract import mode '${ContractImportMode.Validation}' to change the representative package-id."
        ),
        contractImportMode = ContractImportMode.Accept,
      )
    }
  }

  private def testPrecedence(
      knownPackages: Set[LfPackageId],
      expectation: Either[String, LfPackageId],
      contractImportMode: ContractImportMode = ContractImportMode.Validation,
  ): Assertion = {
    import TestValues.*
    inside(
      new SelectRepresentativePackageIds(
        representativePackageIdOverride = RepresentativePackageIdOverride(
          contractOverride = Map(contractId -> contractRpIdOverride),
          packageIdOverride =
            Map(creationPkgId -> creationPackageIdOverride, rpId -> rpIdPackageIdOverride),
          packageNameOverride = Map(packageName -> packageNameOverride),
        ),
        knownPackages = knownPackages,
        packageNameMap = Map(
          packageName -> PackageResolution(
            LocalPackagePreference(
              LfPackageVersion.assertFromString("0.0.0"),
              highestVersionedLocalPackage,
            ),
            allPackageIdsForName = NonEmpty.mk(Set, highestVersionedLocalPackage),
          )
        ),
        contractImportMode = contractImportMode,
        loggerFactory = loggerFactory,
      ).apply(List(repairContract))
    ) {
      case Right(Seq(updatedContract)) =>
        updatedContract.representativePackageId shouldBe expectation.valueOrFail(
          "Right returned on expected Left"
        )
      case Left(error) =>
        error shouldBe expectation.leftOrFail("Left returned on expected Right")
    }
  }

  private def mkContract(
      contractId: Value.ContractId,
      creationPkgId: LfPackageId,
      rpId: LfPackageId,
      pkgName: LfPackageName,
  ) = {
    val templateId =
      Ref.TypeConId.apply(creationPkgId, Ref.QualifiedName.assertFromString("Mod:Template"))

    val signatory = Ref.Party.assertFromString("signatory")
    val observer = Ref.Party.assertFromString("observer")
    RepairContract(
      synchronizerId = SynchronizerId.tryFromString("x::synchronizer"),
      contract = FatContractInstance.fromCreateNode(
        TestNodeBuilder.create(
          id = contractId,
          templateId = templateId,
          argument = ValueUnit,
          signatories = Set(signatory),
          observers = Set(observer),
          packageName = pkgName,
        ),
        createTime = CreationTime.CreatedAt(Time.Timestamp.now()),
        authenticationData = Bytes.Empty,
      ),
      reassignmentCounter = Counter.MinValue,
      representativePackageId = rpId,
    )
  }

  private object TestValues {
    val creationPkgId: LfPackageId = LfPackageId.assertFromString("some-original-pkg-id")
    val rpId: LfPackageId = LfPackageId.assertFromString("some-rp-id")
    val contractId: Value.ContractId = LfContractId.V1(ExampleTransactionFactory.lfHash(1337))
    val packageName: LfPackageName = Ref.PackageName.assertFromString("somePkgName")

    val repairContract: RepairContract = mkContract(contractId, creationPkgId, rpId, packageName)

    val contractRpIdOverride: LfPackageId =
      LfPackageId.assertFromString("some-contract-rpid-override")
    val creationPackageIdOverride: LfPackageId =
      LfPackageId.assertFromString("some-creation-pkg-id-override")
    val rpIdPackageIdOverride: LfPackageId =
      LfPackageId.assertFromString("some-rpid-pkg-id-override")
    val packageNameOverride: LfPackageId =
      LfPackageId.assertFromString("some-package-name-pkg-id-override")
    val highestVersionedLocalPackage: LfPackageId =
      LfPackageId.assertFromString("some-highest-versioned-pkg-id-override")
  }
}
