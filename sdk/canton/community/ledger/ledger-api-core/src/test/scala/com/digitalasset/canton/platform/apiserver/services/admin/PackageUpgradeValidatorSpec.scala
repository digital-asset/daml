// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.digitalasset.canton.config.CachingConfigs
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.topology.TopologyManagerError.ParticipantTopologyManagerError.*
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
  private val packageUpgradeValidator =
    new PackageUpgradeValidator(CachingConfigs.defaultPackageUpgradeCache, loggerFactory)

  protected implicit val loggingContextWithTrace: LoggingContextWithTrace =
    LoggingContextWithTrace.ForTesting

  private val v1: (Ref.PackageId, Ast.PackageSignature) = samplePackageSig(
    packageId = "test-pkg-v1",
    packageName = "TestPkgName",
    packageVersion = "1.0.0",
    discriminatorFields = Seq.empty,
  )

  private val v11Incompatible: (Ref.PackageId, Ast.PackageSignature) = samplePackageSig(
    packageId = "test-pkg-v11",
    packageName = "TestPkgName",
    packageVersion = "1.1.0",
    discriminatorFields = Seq("text : Text"),
  )

  private val v2Compatible: (Ref.PackageId, Ast.PackageSignature) = samplePackageSig(
    packageId = "test-pkg-v2",
    packageName = "TestPkgName",
    packageVersion = "2.0.0",
    discriminatorFields = Seq("party : Option Party"),
  )

  private val v3Incompatible: (Ref.PackageId, Ast.PackageSignature) = samplePackageSig(
    packageId = "test-pkg-v3-incompat",
    packageName = "TestPkgName",
    packageVersion = "3.0.0",
    discriminatorFields = Seq("party : Option Party", "text : Text"),
  )

  private val v3Compatible: (Ref.PackageId, Ast.PackageSignature) = samplePackageSig(
    packageId = "test-pkg-v3-compat",
    packageName = "TestPkgName",
    packageVersion = "3.0.0",
    discriminatorFields = Seq("party : Option Party", "text : Option Text"),
  )

  "validate empty lineage" in {
    val res = validateUpgrade(List.empty, List.empty)
    res shouldBe Right(())
  }

  "validate compatible lineage" in {
    validateUpgrade(List(v1), List.empty) shouldBe Right(())
    validateUpgrade(List(v2Compatible), List(v1)) shouldBe Right(())
    validateUpgrade(List(v1, v2Compatible), List.empty) shouldBe Right(())
  }

  "fail validation of incompatible lineage" in {
    inside(validateUpgrade(List(v3Incompatible), List(v1, v2Compatible))) {
      case Left(error: Upgradeability.Error) =>
        error.newPackage shouldBe Util.PkgIdWithNameAndVersion(v3Incompatible)
        error.oldPackage shouldBe Util.PkgIdWithNameAndVersion(v2Compatible)
    }

    // it does not depend on the vetting order
    inside(validateUpgrade(List(v2Compatible), List(v1, v3Incompatible))) {
      case Left(error: Upgradeability.Error) =>
        error.newPackage shouldBe Util.PkgIdWithNameAndVersion(v3Incompatible)
        error.oldPackage shouldBe Util.PkgIdWithNameAndVersion(v2Compatible)
    }

    inside(validateUpgrade(List(v2Compatible), List(v1, v11Incompatible))) {
      case Left(error: Upgradeability.Error) =>
        error.newPackage shouldBe Util.PkgIdWithNameAndVersion(v11Incompatible)
        error.oldPackage shouldBe Util.PkgIdWithNameAndVersion(v1)
    }

    inside(
      validateUpgrade(List(v11Incompatible), List(v1, v2Compatible, v3Incompatible))
    ) { case Left(error: Upgradeability.Error) =>
      error.newPackage shouldBe Util.PkgIdWithNameAndVersion(v11Incompatible)
      error.oldPackage shouldBe Util.PkgIdWithNameAndVersion(v1)
    }
  }

  "fail validation because of packages with same name and version" in {
    inside(validateUpgrade(List(v3Incompatible), List(v1, v3Compatible))) {
      case Left(error: UpgradeVersion.Error) =>
        Set(error.firstPackage, error.secondPackage) shouldBe Set(
          Util.PkgIdWithNameAndVersion(v3Incompatible),
          Util.PkgIdWithNameAndVersion(v3Compatible),
        )
    }
  }

  private def validateUpgrade(
      newPackages: List[(Ref.PackageId, Ast.PackageSignature)],
      existingPackages: List[(Ref.PackageId, Ast.PackageSignature)],
  ) =
    packageUpgradeValidator.validateUpgrade(
      newPackages.map(_._1).toSet,
      (newPackages ++ existingPackages).map(_._1).toSet,
      (newPackages ++ existingPackages).toMap,
    )

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
