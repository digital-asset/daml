// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.packagemeta

import cats.implicits.catsSyntaxSemigroup
import com.digitalasset.daml.lf.data.Ref
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata.Implicits.packageMetadataSemigroup
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata.{
  LocalPackagePreference,
  PackageResolution,
}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PackageMetadataSpec extends AnyWordSpec with Matchers {

  "PackageMetadata.combine" should {
    "yield the correct result" in new Scope {
      private val pkgMeta1 = PackageMetadata(
        interfaces = Set(interface1, interface2),
        templates = Set(template1A, template1B, template1C),
        interfacesImplementedBy = Map(
          interface1 -> Set(template1A, template1B),
          interface2 -> Set(template1C),
        ),
        packageNameMap = Map(
          pkgName1 -> PackageResolution(
            LocalPackagePreference(pkg1Version1, pkgId1),
            NonEmpty(Set, pkgId1),
          )
        ),
        packageIdVersionMap = Map(pkgId1 -> (pkgName1, pkg1Version1)),
      )

      // Package non-upgradable
      private val pkgMeta2 = PackageMetadata(
        interfaces = Set(interface3),
        templates = Set(template2A),
        interfacesImplementedBy = Map(interface3 -> Set(template2A)),
        packageNameMap = Map.empty,
        packageIdVersionMap = Map.empty,
      )

      private val pkgMeta12 = pkgMeta1 |+| pkgMeta2

      pkgMeta12 shouldBe PackageMetadata(
        interfaces = Set(interface1, interface2, interface3),
        templates = Set(template1A, template1B, template1C, template2A),
        interfacesImplementedBy = Map(
          interface1 -> Set(template1A, template1B),
          interface2 -> Set(template1C),
          interface3 -> Set(template2A),
        ),
        packageNameMap = Map(
          pkgName1 -> PackageResolution(
            LocalPackagePreference(pkg1Version1, pkgId1),
            NonEmpty(Set, pkgId1),
          )
        ),
        packageIdVersionMap = Map(pkgId1 -> (pkgName1, pkg1Version1)),
      )

      private val pkgMeta3 = PackageMetadata(
        templates = Set(template1B_v2),
        interfacesImplementedBy = Map(interface1 -> Set(template1B_v2)),
        packageNameMap = Map(
          pkgName1 -> PackageResolution(
            LocalPackagePreference(pkg1Version2, pkgId3),
            NonEmpty(Set, pkgId3),
          )
        ),
        packageIdVersionMap = Map(pkgId3 -> (pkgName1, pkg1Version2)),
      )

      val pkgMeta123 = pkgMeta12 |+| pkgMeta3
      pkgMeta123 shouldBe PackageMetadata(
        interfaces = Set(interface1, interface2, interface3),
        templates = Set(template1A, template1B, template1C, template2A, template1B_v2),
        interfacesImplementedBy = Map(
          interface1 -> Set(template1A, template1B, template1B_v2),
          interface2 -> Set(template1C),
          interface3 -> Set(template2A),
        ),
        packageNameMap = Map(
          pkgName1 -> PackageResolution(
            LocalPackagePreference(pkg1Version2, pkgId3),
            NonEmpty(Set, pkgId1, pkgId3),
          )
        ),
        packageIdVersionMap = Map(
          pkgId1 -> (pkgName1, pkg1Version1),
          pkgId3 -> (pkgName1, pkg1Version2),
        ),
      )
    }

    "select package-id with higher preference in the package id map" in new Scope {
      private val pkgMeta1 = PackageMetadata(
        packageNameMap = Map(
          pkgName1 -> PackageResolution(
            LocalPackagePreference(pkg1Version1, pkgId1),
            NonEmpty(Set, pkgId1),
          )
        )
      )

      private val pkgMeta2 = PackageMetadata(
        packageNameMap = Map(
          pkgName1 -> PackageResolution(
            LocalPackagePreference(pkg1Version2, pkgId2),
            NonEmpty(Set, pkgId2),
          )
        )
      )

      pkgMeta1 |+| pkgMeta2 shouldBe PackageMetadata(
        packageNameMap = Map(
          pkgName1 -> PackageResolution(
            LocalPackagePreference(pkg1Version2, pkgId2),
            NonEmpty(Set, pkgId1, pkgId2),
          )
        )
      )
    }

    "error on mismatching (package-name, version) updates for the same package-id" in new Scope {
      private val pkgMeta1 = PackageMetadata(
        packageIdVersionMap = Map(pkgId1 -> (pkgName1, pkg1Version1))
      )

      private val pkgMeta2 = PackageMetadata(
        packageIdVersionMap = Map(pkgId1 -> (pkgName1, pkg1Version2))
      )

      intercept[IllegalStateException] {
        pkgMeta1 |+| pkgMeta2
      }.getMessage shouldBe {
        s"Conflicting versioned package names for the same package id $pkgId1. Previous (${(pkgName1, pkg1Version1)}) vs uploaded(${(pkgName1, pkg1Version2)})"
      }
    }
  }

  private trait Scope {
    // Two package name scopes:
    // * pkg1 - upgradable
    // * pkg2 - non-upgradable
    // * pkg3 - upgradable
    //
    // Version 1.1 of pkg1 (pkgId1) is uploaded with:
    //   - interface1 implemented by template1A and template1B
    //   - interface2 implemented by template1C
    // Version 1.1 of pkg2 (pkgId2) is uploaded with:
    //   - interface3 implemented by template2A
    // Version 1.2 of pkg1 (pkgId3) is uploaded with:
    //   - template1B_v2 is a newer version of template1B

    val pkgName1 = Ref.PackageName.assertFromString("pkg1")

    val pkg1Version1 = Ref.PackageVersion.assertFromString("1.1")
    val pkg1Version2 = Ref.PackageVersion.assertFromString("1.2")

    val pkgId1 = Ref.PackageId.assertFromString("PkgId1")
    val pkgId2 = Ref.PackageId.assertFromString("PkgId2")
    val pkgId3 = Ref.PackageId.assertFromString("PkgId3")

    val interface1 = Ref.Identifier.assertFromString(s"$pkgId1:mod:i1")
    val interface2 = Ref.Identifier.assertFromString(s"$pkgId1:mod:i2")
    val interface3 = Ref.Identifier.assertFromString(s"$pkgId2:mod:i3")

    val template1A = Ref.Identifier.assertFromString(s"$pkgId1:mod:t1A")
    val template1B = Ref.Identifier.assertFromString(s"$pkgId1:mod:t1B")
    val template1C = Ref.Identifier.assertFromString(s"$pkgId1:mod:t1C")
    val template2A = Ref.Identifier.assertFromString(s"$pkgId2:mod:t2A")
    val template1B_v2 = Ref.Identifier.assertFromString(s"$pkgId3:mod:t1B")
  }
}
