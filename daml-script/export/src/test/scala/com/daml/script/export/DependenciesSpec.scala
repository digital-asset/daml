// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.lf.language.LanguageVersion._
import com.google.protobuf.ByteString
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class DependenciesSpec extends AnyFreeSpec with Matchers {
  import Dependencies._

  "targetLfVersion" - {
    "empty Seq" in {
      targetLfVersion(Seq.empty) shouldBe None
    }
    "single DALF" in {
      targetLfVersion(Seq(v1_14)) shouldBe Some(v1_14)
    }
    "multiple DALFs" in {
      targetLfVersion(Seq(v1_14, v1_dev)) shouldBe Some(v1_dev)
    }
    "should be at least 1.14" in {
      targetLfVersion(Seq(v1_6)) shouldBe Some(v1_14)
    }
  }
  "targetFlag" - {
    "1.14" in {
      targetFlag(v1_14) shouldBe "--target=1.14"
    }
    "1.dev" in {
      targetFlag(v1_dev) shouldBe "--target=1.dev"
    }
    "2.1" in {
      targetFlag(v2_1) shouldBe "--target=2.1"
    }
    "2.dev" in {
      targetFlag(v2_dev) shouldBe "--target=2.dev"
    }
  }
  "toPackages" - {
    "empty package set" in {
      val pkgId: PackageId = PackageId.assertFromString(
        "e7b2c7155f6dd6fc569c2325be821f1269186a540d0408b9a0c9e30406f6b64b"
      )
      val pkgs: Map[PackageId, (ByteString, Ast.Package)] = Map.empty
      toPackages(pkgId, pkgs) shouldBe None
    }
    "package with metadata" in {
      val pkgId: PackageId = PackageId.assertFromString(
        "e7b2c7155f6dd6fc569c2325be821f1269186a540d0408b9a0c9e30406f6b64b"
      )
      val pkg: Ast.Package = Ast.Package(
        modules = Map.empty[Ref.ModuleName, Ast.GenModule[Ast.Expr]],
        directDeps = Set.empty[PackageId],
        languageVersion = LanguageVersion.v1_8,
        metadata = Some(
          Ast.PackageMetadata(
            name = Ref.PackageName.assertFromString("example-pkg"),
            version = Ref.PackageVersion.assertFromString("1.0.0"),
            None,
          )
        ),
      )
      val pkgs: Map[PackageId, (ByteString, Ast.Package)] = Map((pkgId, (ByteString.EMPTY, pkg)))
      toPackages(pkgId, pkgs) shouldBe Some("example-pkg-1.0.0")
    }
    "package without metadata" in {
      val pkgId: PackageId = PackageId.assertFromString(
        "e7b2c7155f6dd6fc569c2325be821f1269186a540d0408b9a0c9e30406f6b64b"
      )
      val pkg: Ast.Package = Ast.Package(
        modules = Map.empty[Ref.ModuleName, Ast.GenModule[Ast.Expr]],
        directDeps = Set.empty[PackageId],
        languageVersion = LanguageVersion.v1_6,
        metadata = None,
      )
      val pkgs: Map[PackageId, (ByteString, Ast.Package)] = Map((pkgId, (ByteString.EMPTY, pkg)))
      toPackages(pkgId, pkgs) shouldBe Some(
        "e7b2c7155f6dd6fc569c2325be821f1269186a540d0408b9a0c9e30406f6b64b"
      )
    }
    "stable-package with metadata" in {
      val pkgId: PackageId = PackageId.assertFromString(
        "4b36da183ec859061ca972c544c9ebb687e2ee6536d1edd15b04ae0696e607c7"
      )
      val pkg: Ast.Package = Ast.Package(
        modules = Map.empty[Ref.ModuleName, Ast.GenModule[Ast.Expr]],
        directDeps = Set.empty[PackageId],
        languageVersion = LanguageVersion.v1_8,
        metadata = Some(
          Ast.PackageMetadata(
            name = Ref.PackageName.assertFromString("daml-stdlib"),
            version = Ref.PackageVersion.assertFromString("0.0.0"),
            None,
          )
        ),
      )
      val pkgs: Map[PackageId, (ByteString, Ast.Package)] = Map((pkgId, (ByteString.EMPTY, pkg)))
      toPackages(pkgId, pkgs) shouldBe None
    }
    "stable-package without metadata" in {
      // daml-stdlib-DA-Internal-Template
      val pkgId: PackageId = PackageId.assertFromString(
        "d14e08374fc7197d6a0de468c968ae8ba3aadbf9315476fd39071831f5923662"
      )
      val pkg: Ast.Package = Ast.Package(
        modules = Map.empty[Ref.ModuleName, Ast.GenModule[Ast.Expr]],
        directDeps = Set.empty[PackageId],
        languageVersion = LanguageVersion.v1_8,
        metadata = None,
      )
      val pkgs: Map[PackageId, (ByteString, Ast.Package)] = Map((pkgId, (ByteString.EMPTY, pkg)))
      toPackages(pkgId, pkgs) shouldBe None
    }
  }
}
