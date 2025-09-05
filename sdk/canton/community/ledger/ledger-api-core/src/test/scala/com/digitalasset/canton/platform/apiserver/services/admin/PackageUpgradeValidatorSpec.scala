// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.topology.TopologyManagerError
import com.digitalasset.canton.{BaseTest, FailOnShutdown, LfPackageName, LfPackageVersion}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.{Ast, Util}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class PackageUpgradeValidatorSpec
    extends AsyncWordSpec
    with FailOnShutdown
    with Matchers
    with BaseTest {
  private val packageUpgradeValidator = new PackageUpgradeValidator(loggerFactory)

  protected implicit val loggingContextWithTrace: LoggingContextWithTrace =
    LoggingContextWithTrace.ForTesting

  private val v1: (Ref.PackageId, Ast.PackageSignature) = samplePackageSig(
    packageId = "test-pkg-v1",
    packageName = "TestPkgName",
    packageVersion = "1.0.0",
    discriminatorFields = Seq.empty,
  )

  private val v2Compatible: (Ref.PackageId, Ast.PackageSignature) = samplePackageSig(
    packageId = "test-pkg-v2",
    packageName = "TestPkgName",
    packageVersion = "2.0.0",
    discriminatorFields = Seq("party: Option Party"),
  )

  private val v3Incompatible: (Ref.PackageId, Ast.PackageSignature) = samplePackageSig(
    packageId = "test-pkg-v3",
    packageName = "TestPkgName",
    packageVersion = "3.0.0",
    discriminatorFields = Seq("text: Text"),
  )

  "validate empty upgrade" in {
    for {
      res1 <- packageUpgradeValidator.validateUpgrade(List.empty, Map.empty).value
      res2 <- packageUpgradeValidator.validateUpgrade(List.empty, Map(v1)).value
    } yield {
      res1 shouldBe Right(())
      res2 shouldBe Right(())
    }
  }

  "validate compatible upgrade" in {
    for {
      res1 <- packageUpgradeValidator.validateUpgrade(List(v1), Map.empty).value
      res2 <- packageUpgradeValidator.validateUpgrade(List(v2Compatible), Map(v1)).value
    } yield {
      res1 shouldBe Right(())
      res2 shouldBe Right(())
    }
  }

  "validate compatible downgrade" in {
    for {
      res1 <- packageUpgradeValidator.validateUpgrade(List(v1), Map(v2Compatible)).value
    } yield {
      res1 shouldBe Right(())
    }
  }

  "validate incompatible upgrade" in {
    for {
      res1 <- packageUpgradeValidator
        .validateUpgrade(List(v3Incompatible), Map(v1, v2Compatible))
        .value
    } yield {
      inside(res1) {
        case Left(
              error: TopologyManagerError.ParticipantTopologyManagerError.Upgradeability.Error
            ) =>
          error.newPackage shouldBe Util.PkgIdWithNameAndVersion(v3Incompatible)
          error.oldPackage shouldBe Util.PkgIdWithNameAndVersion(v2Compatible)
          error.isUpgradeCheck shouldBe true
      }
    }
  }

  "validate incompatible downgrade" in {
    for {
      res1 <- packageUpgradeValidator
        .validateUpgrade(List(v2Compatible), Map(v1, v3Incompatible))
        .value
    } yield {
      inside(res1) {
        case Left(
              error: TopologyManagerError.ParticipantTopologyManagerError.Upgradeability.Error
            ) =>
          error.newPackage shouldBe Util.PkgIdWithNameAndVersion(v3Incompatible)
          error.oldPackage shouldBe Util.PkgIdWithNameAndVersion(v2Compatible)
          error.isUpgradeCheck shouldBe false
      }
    }
  }

  private def samplePackageSig(
      packageId: String,
      packageName: String,
      packageVersion: String,
      discriminatorFields: Seq[String],
  ): (Ref.PackageId, Ast.PackageSignature) = {
    val refPackageId = Ref.PackageId.assertFromString(packageId)
    val astPackage = PackageTestUtils
      .sampleAstPackage(
        packageName = LfPackageName.assertFromString(packageName),
        packageVersion = LfPackageVersion.assertFromString(packageVersion),
        discriminatorFields = discriminatorFields,
      )(packageId = refPackageId)
    refPackageId -> Util.toSignature(astPackage)
  }
}
