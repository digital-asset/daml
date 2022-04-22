// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen

import java.nio.file.Path

import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.archive.DarReader
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref._
import com.daml.lf.iface._
import com.daml.lf.codegen.conf.PackageReference
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

final class CodeGenRunnerTests extends AnyFlatSpec with Matchers {

  import CodeGenRunnerTests._

  behavior of "configureCodeGenScope"

  it should "read interfaces from a single DAR file without a prefix" in {

    val scope = CodeGenRunner.configureCodeGenScope(Map(testDar -> None), Map.empty)

    assert(scope.signatures.length === 25)
    assert(scope.packagePrefixes === Map.empty)
    assert(scope.toBeGenerated === Set.empty)
  }

  it should "read interfaces from a single DAR file with a prefix" in {

    val scope = CodeGenRunner.configureCodeGenScope(Map(testDar -> Some("PREFIX")), Map.empty)

    assert(scope.signatures.map(_.packageId).length === dar.all.length)
    assert(scope.packagePrefixes.size === dar.all.length)
    assert(scope.packagePrefixes.values.forall(_ === "PREFIX"))
    assert(scope.toBeGenerated === Set.empty)
  }

  behavior of "detectModuleCollisions"

  it should "succeed if there are no collisions" in {
    assert(
      CodeGenRunner.detectModuleCollisions(
        Map.empty,
        Seq(interface("pkg1", "A", "A.B"), interface("pkg2", "B", "A.B.C")),
      ) === ()
    )
  }

  it should "fail if there is a collision" in {
    assertThrows[IllegalArgumentException] {
      CodeGenRunner.detectModuleCollisions(
        Map.empty,
        Seq(interface("pkg1", "A"), interface("pkg2", "A")),
      )
    }
  }

  it should "fail if there is a collision caused by prefixing" in {
    assertThrows[IllegalArgumentException] {
      CodeGenRunner.detectModuleCollisions(
        Map(PackageId.assertFromString("pkg2") -> "A"),
        Seq(interface("pkg1", "A.B"), interface("pkg2", "B")),
      )
    }
  }

  it should "succeed if collision is resolved by prefixing" in {
    assert(
      CodeGenRunner.detectModuleCollisions(
        Map(PackageId.assertFromString("pkg2") -> "Pkg2"),
        Seq(interface("pkg1", "A"), interface("pkg2", "A")),
      ) === ()
    )
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
    assert(
      CodeGenRunner.resolvePackagePrefixes(
        pkgPrefixes,
        modulePrefixes,
        Seq(interface1, interface2, interface3),
      ) ===
        Map(pkg1 -> "com.pkg1", pkg2 -> "com.pkg2.a.b", pkg3 -> "c.d")
    )
  }
  it should "fail if module-prefixes references non-existing package" in {
    val name2 = PackageName.assertFromString("name2")
    val version = PackageVersion.assertFromString("1.0.0")
    val modulePrefixes =
      Map[PackageReference, String](PackageReference.NameVersion(name2, version) -> "A.B")
    assertThrows[IllegalArgumentException] {
      CodeGenRunner.resolvePackagePrefixes(Map.empty, modulePrefixes, Seq.empty)
    }
  }
}

object CodeGenRunnerTests {

  private[this] val testDarPath = "language-support/java/codegen/test-daml.dar"
  private val testDar = Path.of(BazelRunfiles.rlocation(testDarPath))
  private val dar = DarReader.assertReadArchiveFromFile(testDar.toFile)

  private def interface(pkgId: String, modNames: String*): Interface =
    interface(pkgId, None, modNames: _*)

  private def interface(
      pkgId: String,
      metadata: Option[PackageMetadata],
      modNames: String*
  ): Interface = {
    val dummyType = InterfaceType.Normal(DefDataType(ImmArraySeq.empty, Record(ImmArraySeq.empty)))
    Interface(
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
