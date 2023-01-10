// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen

import java.nio.file.Path
import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.archive.DarReader
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref._
import com.daml.lf.typesig._
import com.daml.lf.codegen.conf.PackageReference
import com.daml.lf.language.Reference
import com.daml.lf.language.StablePackage
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

final class CodeGenRunnerTests extends AnyFlatSpec with Matchers {

  import CodeGenRunnerTests._

  behavior of "configureCodeGenScope"

  val stablePackageIds = StablePackage.values.map(_.packageId)

  it should "read interfaces from a single DAR file without a prefix" in {

    val scope = CodeGenRunner.configureCodeGenScope(Map(testDar -> None), Map.empty)

    // `daml-prim` + `daml-stdlib` + testDar
    scope.signatures.map(_.packageId).diff(stablePackageIds).length should ===(3)
    scope.packagePrefixes should ===(Map.empty)
    scope.toBeGenerated should ===(Set.empty)
  }

  it should "read interfaces from 2 DAR files with same dependencies without a prefix" in {

    val scope =
      CodeGenRunner.configureCodeGenScope(
        Map(testDar -> None, testDarWithSameDependencies -> None),
        Map.empty,
      )

    // `daml-prim` + `daml-stdlib` + testDar + testDarWithSameDependencies
    scope.signatures.map(_.packageId).diff(stablePackageIds).length should ===(4)
    scope.packagePrefixes should ===(Map.empty)
    scope.toBeGenerated should ===(Set.empty)
  }

  // Test case reproducing #15341
  it should "read interfaces from 2 DAR files with same dependencies but one with different daml compiler version" in {

    val scope =
      CodeGenRunner.configureCodeGenScope(
        Map(testDar -> None, testDarWithSameDependenciesButDifferentTargetVersion -> None),
        Map.empty,
      )

    // `daml-prim`
    // + `daml-stdlib`
    // + testDar
    // + `daml-prim` from different LF version
    // + `daml-stdlib` from different LF version
    // + testDarWithSameDependenciesButDifferentTargetVersion
    scope.signatures.map(_.packageId).diff(stablePackageIds).length should ===(6)
    scope.packagePrefixes should ===(Map.empty)
    scope.toBeGenerated should ===(Set.empty)
  }

  it should "read interfaces from a single DAR file with a prefix" in {

    val scope = CodeGenRunner.configureCodeGenScope(Map(testDar -> Some("prefix")), Map.empty)

    scope.signatures.map(_.packageId).length should ===(dar.all.length)
    val prefixes = backend.java.inner.PackagePrefixes unwrap scope.packagePrefixes
    prefixes.size should ===(dar.all.length)
    all(prefixes.values) should ===("prefix")
    scope.toBeGenerated should ===(Set.empty)
  }

  it should "read interfaces from 2 DAR files with same content and same prefixes" in {

    val scope =
      CodeGenRunner.configureCodeGenScope(
        Map(testDar -> Some("prefix"), testDarWithSameSrcAndProjectNamePathDar -> Some("prefix")),
        Map.empty,
      )

    scope.signatures.map(_.packageId).length should ===(dar.all.length)
    val prefixes = backend.java.inner.PackagePrefixes unwrap scope.packagePrefixes
    prefixes.size should ===(dar.all.length)
    all(prefixes.values) should ===("prefix")
    scope.toBeGenerated should ===(Set.empty)
  }

  it should "fail if read interfaces from 2 DAR files with same content but different prefixes" in {
    assertThrows[IllegalArgumentException] {
      CodeGenRunner.configureCodeGenScope(
        Map(testDar -> Some("prefix1"), testDarWithSameSrcAndProjectNamePathDar -> Some("prefix2")),
        Map.empty,
      )
    }
  }

  it should "read interfaces from 2 DAR files with one is depending on other packages using data_dependencies" in {

    val scope = CodeGenRunner.configureCodeGenScope(
      Map(testTemplateDar -> Some("prefix1"), testDependsOnBarTplDar -> Some("prefix2")),
      Map.empty,
    )

    // `daml-prim`
    //  + `daml-stdlib`
    //  + testTemplateDar
    //  + testDependsOnBarTplDar
    //  + `test-another-bar.dar`
    scope.signatures.map(_.packageId).diff(stablePackageIds).length should ===(5)
    val prefixes = backend.java.inner.PackagePrefixes unwrap scope.packagePrefixes
    prefixes.size should ===(3)
    // prefix1 is applied to the main package containing template Bar
    prefixes.values.count(_ == "prefix1") should ===(1)
    // prefix2 is applied to the main package containing template UsingBar
    // and the unique package containing template AnotherBar
    prefixes.values.count(_ == "prefix2") should ===(2)
  }

  behavior of "detectModuleCollisions"

  private def moduleIdSet(signatures: Seq[PackageSignature]): Set[Reference.Module] = {
    (for {
      s <- signatures
      module <- s.typeDecls.keySet.map(_.module)
    } yield Reference.Module(s.packageId, module)).toSet
  }

  it should "succeed if there are no collisions" in {
    val signatures = Seq(interface("pkg1", "A", "A.B"), interface("pkg2", "B", "A.B.C"))
    CodeGenRunner.detectModuleCollisions(
      Map.empty,
      signatures,
      moduleIdSet(signatures),
    ) should ===(())
  }

  it should "fail if there is a collision" in {
    val signatures = Seq(interface("pkg1", "A"), interface("pkg2", "A"))
    assertThrows[IllegalArgumentException] {
      CodeGenRunner.detectModuleCollisions(
        Map.empty,
        signatures,
        moduleIdSet(signatures),
      )
    }
  }

  it should "fail if there is a collision caused by prefixing" in {
    val signatures = Seq(interface("pkg1", "A.B"), interface("pkg2", "B"))
    assertThrows[IllegalArgumentException] {
      CodeGenRunner.detectModuleCollisions(
        Map(PackageId.assertFromString("pkg2") -> "A"),
        Seq(interface("pkg1", "A.B"), interface("pkg2", "B")),
        moduleIdSet(signatures),
      )
    }
  }

  it should "succeed if collision is resolved by prefixing" in {
    val signatures = Seq(interface("pkg1", "A"), interface("pkg2", "A"))
    CodeGenRunner.detectModuleCollisions(
      Map(PackageId.assertFromString("pkg2") -> "Pkg2"),
      signatures,
      moduleIdSet(signatures),
    ) should ===(())
  }

  it should "succeed if there is a collisions on modules which are not to be generated" in {
    val signatures = Seq(interface("pkg1", "A"), interface("pkg2", "A"))
    CodeGenRunner.detectModuleCollisions(
      Map.empty,
      signatures,
      Set.empty,
    ) should ===(())
  }

  it should "succeed if same module name between a module not to be generated and a module to be generated " in {
    val signatures = Seq(interface("pkg1", "A"), interface("pkg2", "A"))
    CodeGenRunner.detectModuleCollisions(
      Map.empty,
      signatures,
      Set(Reference.Module(PackageId.assertFromString("pkg1"), ModuleName.assertFromString("A"))),
    ) should ===(())
  }

  behavior of "resolvePackagePrefixes"

  it should "combine module-prefixes and pkgPrefixes" in {
    val pkg1 = PackageId.assertFromString("pkg-1")
    val pkg2 = PackageId.assertFromString("pkg-2")
    val pkg3 = PackageId.assertFromString("pkg-3")
    val pkgPrefixes = Map(pkg1 -> "com.pkg1", pkg2 -> "com.pkg2")
    val name2 = PackageName.assertFromString("name2")
    val name3 = PackageName.assertFromString("name3")
    val version = PackageVersion.assertFromString("1.0.0")
    val modulePrefixes = Map[PackageReference, String](
      PackageReference.NameVersion(name2, version) -> "A.B",
      PackageReference.NameVersion(name3, version) -> "C.D",
    )
    val interface1 = interface(pkg1, None)
    val interface2 = interface(pkg2, Some(PackageMetadata(name2, version)))
    val interface3 = interface(pkg3, Some(PackageMetadata(name3, version)))
    CodeGenRunner.resolvePackagePrefixes(
      pkgPrefixes,
      modulePrefixes,
      Seq(interface1, interface2, interface3),
      moduleIdSet(Seq(interface1, interface2, interface3)),
    ) should ===(Map(pkg1 -> "com.pkg1", pkg2 -> "com.pkg2.a.b", pkg3 -> "c.d"))
  }

  it should "fail if module-prefixes references non-existing package" in {
    val name2 = PackageName.assertFromString("name2")
    val version = PackageVersion.assertFromString("1.0.0")
    val modulePrefixes =
      Map[PackageReference, String](PackageReference.NameVersion(name2, version) -> "A.B")
    assertThrows[IllegalArgumentException] {
      CodeGenRunner.resolvePackagePrefixes(Map.empty, modulePrefixes, Seq.empty, Set.empty)
    }
  }
}

object CodeGenRunnerTests {

  private[this] val testDarPath = "language-support/java/codegen/test-daml.dar"
  private[this] val testDarWithSameDependenciesPath =
    "language-support/java/codegen/test-daml-with-same-dependencies.dar"
  private[this] val testDarWithSameSrcAndProjectNamePath =
    "language-support/java/codegen/test-daml-with-same-source-project-name.dar"
  private[this] val testDarWithSameDependenciesButDifferentTargetVersionPath =
    "language-support/java/codegen/test-daml-with-same-dependencies-but-different-target-version.dar"
  private[this] val testTemplateDarPath = "language-support/java/codegen/test-template.dar"
  private[this] val testDependsOnBarTplDarPath =
    "language-support/java/codegen/test-depending-on-bar-template.dar"

  private val testDar = Path.of(BazelRunfiles.rlocation(testDarPath))
  private val testDarWithSameDependencies =
    Path.of(BazelRunfiles.rlocation(testDarWithSameDependenciesPath))
  private val testDarWithSameSrcAndProjectNamePathDar =
    Path.of(BazelRunfiles.rlocation(testDarWithSameSrcAndProjectNamePath))
  private val testDarWithSameDependenciesButDifferentTargetVersion =
    Path.of(BazelRunfiles.rlocation(testDarWithSameDependenciesButDifferentTargetVersionPath))
  private val testTemplateDar = Path.of(BazelRunfiles.rlocation(testTemplateDarPath))
  private val testDependsOnBarTplDar = Path.of(BazelRunfiles.rlocation(testDependsOnBarTplDarPath))
  private val dar = DarReader.assertReadArchiveFromFile(testDar.toFile)

  private def interface(pkgId: String, modNames: String*): PackageSignature =
    interface(pkgId, None, modNames: _*)

  private def interface(
      pkgId: String,
      metadata: Option[PackageMetadata],
      modNames: String*
  ): PackageSignature = {
    val dummyType =
      PackageSignature.TypeDecl.Normal(DefDataType(ImmArraySeq.empty, Record(ImmArraySeq.empty)))
    PackageSignature(
      PackageId.assertFromString(pkgId),
      metadata,
      modNames.view
        .map(n =>
          QualifiedName(
            ModuleName.assertFromString(n),
            DottedName.assertFromString("Dummy"),
          ) -> dummyType
        )
        .toMap,
      Map.empty,
    )
  }

}
