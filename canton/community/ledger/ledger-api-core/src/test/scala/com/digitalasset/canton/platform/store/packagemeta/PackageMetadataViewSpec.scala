// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.packagemeta

import cats.implicits.catsSyntaxSemigroup
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.value.test.ValueGenerators.idGen
import com.daml.nonempty.NonEmptyUtil
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata.{
  TemplateIdWithPriority,
  TemplatesForQualifiedName,
}
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadataViewSpec.*
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import PackageMetadata.Implicits.packageMetadataSemigroup

class PackageMetadataViewSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  "PackageMetadataView" should "append" in forAll(packageMetadataGen, packageMetadataGen) {
    (pkgMeta1, pkgMeta2) =>
      val view = PackageMetadataView.create
      view.current() shouldBe PackageMetadata()

      view.update(pkgMeta1)
      view.current() shouldBe pkgMeta1

      view.update(pkgMeta2)
      view.current() shouldBe pkgMeta1 |+| pkgMeta2
  }
}

object PackageMetadataViewSpec {
  private def entryGen: Gen[(Ref.Identifier, Set[Ref.Identifier])] =
    for {
      key <- idGen
      values <- Gen.listOf(idGen)
    } yield (key, values.toSet)

  private def interfacesImplementedByMap: Gen[Map[Ref.Identifier, Set[Ref.Identifier]]] =
    for {
      entries <- Gen.listOf(entryGen)
    } yield entries.toMap

  private def priorityGen: Gen[Time.Timestamp] =
    Gen.choose(1L, 1L << 20).map(Time.Timestamp.assertFromLong)

  private val packageMetadataGen = for {
    map <- interfacesImplementedByMap
    priority <- priorityGen
  } yield PackageMetadata(
    templates = map.keySet.groupBy(_.qualifiedName).map { case (qn, templateIds) =>
      qn -> TemplatesForQualifiedName(
        NonEmptyUtil.fromUnsafe(templateIds),
        TemplateIdWithPriority(templateIds.head, priority),
      )
    },
    interfaces = map.values.flatten.toSet,
    interfacesImplementedBy = map,
  )
}
