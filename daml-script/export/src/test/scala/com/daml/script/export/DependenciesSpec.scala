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
      targetLfVersion(Seq(v2_1)) shouldBe Some(v2_1)
    }
    "multiple DALFs" in {
      targetLfVersion(Seq(v2_1, v2_dev)) shouldBe Some(v2_dev)
    }
  }
  "targetFlag" - {
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
        languageVersion = LanguageVersion.v2_1,
        metadata = Ast.PackageMetadata(
          name = Ref.PackageName.assertFromString("example-pkg"),
          version = Ref.PackageVersion.assertFromString("1.0.0"),
          None,
        ),
      )
      val pkgs: Map[PackageId, (ByteString, Ast.Package)] = Map((pkgId, (ByteString.EMPTY, pkg)))
      toPackages(pkgId, pkgs) shouldBe Some("example-pkg-1.0.0")
    }
    "stable-package with metadata" in {
      val pkgId: PackageId = PackageId.assertFromString(
        "4b36da183ec859061ca972c544c9ebb687e2ee6536d1edd15b04ae0696e607c7"
      )
      val pkg: Ast.Package = Ast.Package(
        modules = Map.empty[Ref.ModuleName, Ast.GenModule[Ast.Expr]],
        directDeps = Set.empty[PackageId],
        languageVersion = LanguageVersion.v2_1,
        metadata = Ast.PackageMetadata(
          name = Ref.PackageName.assertFromString("daml-stdlib"),
          version = Ref.PackageVersion.assertFromString("0.0.0"),
          None,
        ),
      )
      val pkgs: Map[PackageId, (ByteString, Ast.Package)] = Map((pkgId, (ByteString.EMPTY, pkg)))
      toPackages(pkgId, pkgs) shouldBe None
    }
  }
}
