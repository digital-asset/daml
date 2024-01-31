// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.packagemeta

import cats.implicits.catsSyntaxSemigroup
import com.daml.lf.data.Ref
import com.daml.lf.value.test.ValueGenerators.idGen
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata.Implicits.packageMetadataSemigroup
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadataViewSpec.*
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

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

  private val packageMetadataGen = for {
    map <- interfacesImplementedByMap
  } yield PackageMetadata(
    templates = map.values.flatten.toSet,
    interfaces = map.keySet,
    interfacesImplementedBy = map,
  )
}
