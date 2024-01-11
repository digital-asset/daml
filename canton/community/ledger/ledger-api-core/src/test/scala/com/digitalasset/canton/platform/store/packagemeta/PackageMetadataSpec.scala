// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.packagemeta

import cats.kernel.Semigroup
import com.daml.lf.data.{Ref, Time}
import com.daml.nonempty.NonEmptyUtil
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata.Implicits.packageMetadataSemigroup
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata.{
  TemplateIdWithPriority,
  TemplatesForQualifiedName,
}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PackageMetadataSpec extends AnyWordSpec with Matchers {

  "PackageMetadata.createVersionedTemplatesMap" should {
    "create the correct versioned templates map" in new Scope {
      private val priority = Time.Timestamp.assertFromLong(1337L)
      private val definedTemplates = Set(templateId2_interface1, templateId1_interface2)

      private val expected = Map(
        templateId2_interface1_qualifiedName -> TemplatesForQualifiedName(
          NonEmptyUtil.fromUnsafe(Set(templateId2_interface1)),
          TemplateIdWithPriority(templateId2_interface1, priority),
        ),
        templateId1_interface2_qualifiedName -> TemplatesForQualifiedName(
          NonEmptyUtil.fromUnsafe(Set(templateId1_interface2)),
          TemplateIdWithPriority(templateId1_interface2, priority),
        ),
      )
      private val actual = PackageMetadata.createVersionedTemplatesMap(definedTemplates, priority)
      actual shouldBe expected
    }
  }

  "PackageMetadata.combine" should {
    "yield the correct result" in new Scope {
      private val pkgMeta1 = PackageMetadata(
        templates = Map(
          templateId1_interface1_qualifiedName -> TemplatesForQualifiedName(
            NonEmptyUtil.fromUnsafe(Set(templateId1_interface1)),
            TemplateIdWithPriority(templateId1_interface1, tstampPkg1),
          ),
          templateId2_interface1_qualifiedName -> TemplatesForQualifiedName(
            NonEmptyUtil.fromUnsafe(Set(templateId2_interface1)),
            TemplateIdWithPriority(templateId2_interface1, tstampPkg1),
          ),
          templateId1_interface2_qualifiedName -> TemplatesForQualifiedName(
            NonEmptyUtil.fromUnsafe(Set(templateId1_interface2)),
            TemplateIdWithPriority(templateId1_interface2, tstampPkg1),
          ),
        ),
        interfaces = Set(interface1, interface2),
        interfacesImplementedBy = Map(
          interface1 -> Set(templateId1_interface1, templateId2_interface1),
          interface2 -> Set(templateId1_interface2),
        ),
      )

      private val pkgMeta2 = PackageMetadata(
        templates = Map(
          templateId3_interface1_qualifiedName -> TemplatesForQualifiedName(
            NonEmptyUtil.fromUnsafe(Set(templateId3_interface1)),
            TemplateIdWithPriority(templateId3_interface1, tstampPkg2),
          ),
          templateId2_interface1_qualifiedName -> TemplatesForQualifiedName(
            NonEmptyUtil.fromUnsafe(Set(templateId2_interface1_v2)),
            TemplateIdWithPriority(templateId2_interface1_v2, tstampPkg2),
          ),
          templateId1_interface3_qualifiedName -> TemplatesForQualifiedName(
            NonEmptyUtil.fromUnsafe(Set(templateId1_interface3)),
            TemplateIdWithPriority(templateId1_interface3, tstampPkg2),
          ),
        ),
        interfaces = Set(interface1, interface3),
        interfacesImplementedBy = Map(
          interface1 -> Set(templateId3_interface1, templateId2_interface1_v2),
          interface3 -> Set(templateId1_interface3),
        ),
      )

      private val pkgMeta3 = PackageMetadata(
        templates = Map(
          templateId2_interface1_qualifiedName -> TemplatesForQualifiedName(
            NonEmptyUtil.fromUnsafe(Set(templateId2_interface1_v0)),
            TemplateIdWithPriority(templateId2_interface1_v0, tstampPkg3),
          )
        ),
        interfaces = Set(interface1),
        interfacesImplementedBy = Map(interface1 -> Set(templateId2_interface1_v0)),
      )

      // Note: Preference has been taken to use this explicit Semigroup combine test for two reasons:
      //         1. It's easier for the reader to understand the scenario
      //         2. The Cats laws Semigroup generator for Scalacheck is hard to tune
      //            for complex datastructures like this
      //            since by default the test runtime is ~1 minute.
      private val pkgMeta12 = Semigroup.combine(pkgMeta1, pkgMeta2)

      pkgMeta12 shouldBe PackageMetadata(
        templates = Map(
          templateId1_interface1_qualifiedName -> TemplatesForQualifiedName(
            NonEmptyUtil.fromUnsafe(Set(templateId1_interface1)),
            TemplateIdWithPriority(templateId1_interface1, tstampPkg1),
          ),
          templateId2_interface1_qualifiedName -> TemplatesForQualifiedName(
            // Multiple versions for same qualified name (newer takes precedence)
            NonEmptyUtil.fromUnsafe(Set(templateId2_interface1, templateId2_interface1_v2)),
            TemplateIdWithPriority(templateId2_interface1_v2, tstampPkg2),
          ),
          templateId1_interface2_qualifiedName -> TemplatesForQualifiedName(
            NonEmptyUtil.fromUnsafe(Set(templateId1_interface2)),
            TemplateIdWithPriority(templateId1_interface2, tstampPkg1),
          ),
          templateId1_interface3_qualifiedName -> TemplatesForQualifiedName(
            NonEmptyUtil.fromUnsafe(Set(templateId1_interface3)),
            TemplateIdWithPriority(templateId1_interface3, tstampPkg2),
          ),
          templateId3_interface1_qualifiedName -> TemplatesForQualifiedName(
            NonEmptyUtil.fromUnsafe(Set(templateId3_interface1)),
            TemplateIdWithPriority(templateId3_interface1, tstampPkg2),
          ),
        ),
        interfaces = Set(interface1, interface2, interface3),
        interfacesImplementedBy = Map(
          interface1 -> Set(
            templateId1_interface1,
            templateId2_interface1,
            templateId2_interface1_v2,
            templateId3_interface1,
          ),
          interface2 -> Set(templateId1_interface2),
          interface3 -> Set(templateId1_interface3),
        ),
      )

      private val pkgMeta123_actual = Semigroup.combine(pkgMeta12, pkgMeta3)
      private val pkgMeta123_expected: PackageMetadata = pkgMeta12.copy(
        templates = pkgMeta12.templates.updated(
          templateId2_interface1_qualifiedName,
          TemplatesForQualifiedName(
            NonEmptyUtil.fromUnsafe(
              Set(templateId2_interface1_v0, templateId2_interface1, templateId2_interface1_v2)
            ),
            TemplateIdWithPriority(templateId2_interface1_v2, tstampPkg2),
          ),
        ),
        interfacesImplementedBy = pkgMeta12.interfacesImplementedBy.updated(
          interface1,
          Set(
            templateId1_interface1,
            templateId2_interface1_v0,
            templateId2_interface1,
            templateId2_interface1_v2,
            templateId3_interface1,
          ),
        ),
      )

      pkgMeta123_actual shouldBe pkgMeta123_expected
    }
  }

  private trait Scope {
    // The following identifiers simulate this scenario:
    // * Package 1 is uploaded with
    //   - interface 1 implemented by templateId1_interface1 and templateId2_interface1
    //   - interface 2 implemented by templateId1_interface2
    // * Package 2 (with higher priority) is uploaded with
    //   - interface 1 is extended with new implementation for templateId3_interface1
    //   - templateId2_interface1_v2 is a newer version of templateId2_interface1
    //   - interface 3 implemented by templateId1_interface3
    // * Package 3 (with lower priority) is uploaded with
    //   - templateId2_interface1_v3 is an older version of templateId2_interface1

    val pkgId1 = Ref.PackageId.assertFromString("Pkg1")
    val pkgId2 = Ref.PackageId.assertFromString("Pkg2")
    val pkgId3 = Ref.PackageId.assertFromString("Pkg3")

    val tstampPkg1 = Time.Timestamp.assertFromLong(2L)
    val tstampPkg2 = Time.Timestamp.assertFromLong(3L)
    val tstampPkg3 = Time.Timestamp.assertFromLong(1L)

    val interface1 = Ref.Identifier.assertFromString("Pkg1:mod:i1")
    val interface2 = Ref.Identifier.assertFromString("Pkg1:mod:i2")
    val interface3 = Ref.Identifier.assertFromString("Pkg2:mod:i3")

    val templateId1_interface1_qualifiedName = Ref.QualifiedName.assertFromString("mod:i1t1")
    val templateId2_interface1_qualifiedName = Ref.QualifiedName.assertFromString("mod:i1t2")
    val templateId1_interface2_qualifiedName = Ref.QualifiedName.assertFromString("mod:i2t1")
    val templateId3_interface1_qualifiedName = Ref.QualifiedName.assertFromString("mod:i1t3")
    val templateId1_interface3_qualifiedName = Ref.QualifiedName.assertFromString("mod:i3t1")

    val templateId1_interface1 = Ref.Identifier(pkgId1, templateId1_interface1_qualifiedName)
    val templateId2_interface1 = Ref.Identifier(pkgId1, templateId2_interface1_qualifiedName)
    val templateId1_interface2 = Ref.Identifier(pkgId1, templateId1_interface2_qualifiedName)
    val templateId3_interface1 = Ref.Identifier(pkgId2, templateId3_interface1_qualifiedName)
    val templateId1_interface3 = Ref.Identifier(pkgId2, templateId1_interface3_qualifiedName)
    val templateId2_interface1_v2 = Ref.Identifier(pkgId2, templateId2_interface1_qualifiedName)
    val templateId2_interface1_v0 = Ref.Identifier(pkgId3, templateId2_interface1_qualifiedName)
  }
}
