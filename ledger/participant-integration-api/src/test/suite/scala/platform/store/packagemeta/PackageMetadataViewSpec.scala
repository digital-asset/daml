// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.packagemeta

import com.daml.lf.data.Ref
import com.daml.platform.store.packagemeta.PackageMetadataView.PackageMetadata
import com.daml.scalatest.FlatSpecCheckLaws
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scalaz.scalacheck.ScalazProperties
import com.daml.lf.value.test.ValueGenerators.idGen
import com.daml.platform.store.packagemeta.PackageMetadataViewSpec._
import org.scalatest.EitherValues
import scalaz.{Equal, Monoid}

class PackageMetadataViewSpec
    extends AnyFlatSpec
    with Matchers
    with FlatSpecCheckLaws
    with EitherValues {

  behavior of "MetadataDefinitions Monoid"
  checkLaws(
    ScalazProperties.monoid
      .laws[PackageMetadata](monoidPackageMetadata, equalPackageMetadata, packageMetadataArbitrary)
  )

  behavior of "PackageMetadataView"

  it should "noop in case of empty MetadataDefinitions" in new Scope {
    view.current() shouldBe PackageMetadata()
    view.update(PackageMetadata())
    view.current() shouldBe PackageMetadata()
  }

  it should "append templates" in new Scope {
    view.current() shouldBe PackageMetadata()
    view.update(PackageMetadata(templates = Set(template1)))
    view.current() shouldBe PackageMetadata(templates = Set(template1))
  }

  it should "append interfaces" in new Scope {
    view.current() shouldBe PackageMetadata()
    view.update(PackageMetadata(interfaces = Set(iface1)))
    view.current() shouldBe PackageMetadata(interfaces = Set(iface1))
  }

  it should "append interface to templates map" in new Scope {
    view.current() shouldBe PackageMetadata()
    view.update(
      PackageMetadata(interfacesImplementedBy = Map(iface1 -> Set(template1, template2)))
    )
    view.current() shouldBe PackageMetadata(
      interfacesImplementedBy = Map(iface1 -> Set(template1, template2))
    )
  }

  it should "append interface to templates by key" in new Scope {
    view.current() shouldBe PackageMetadata()
    view.update(
      PackageMetadata(interfacesImplementedBy = Map(iface1 -> Set(template1)))
    )
    view.update(
      PackageMetadata(interfacesImplementedBy = Map(iface1 -> Set(template2)))
    )
    view.current() shouldBe PackageMetadata(
      interfacesImplementedBy = Map(iface1 -> Set(template1, template2))
    )
  }

  it should "append additional interfaces to interface map" in new Scope {
    view.current() shouldBe PackageMetadata()
    view.update(
      PackageMetadata(interfacesImplementedBy = Map(iface1 -> Set(template1, template2)))
    )
    view.update(
      PackageMetadata(interfacesImplementedBy = Map(iface2 -> Set(template1)))
    )
    view.current() shouldBe PackageMetadata(
      interfacesImplementedBy = Map(
        iface1 -> Set(template1, template2),
        iface2 -> Set(template1),
      )
    )
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
  } yield PackageMetadata(map.keySet, map.values.flatten.toSet, map)

  implicit val monoidPackageMetadata = new Monoid[PackageMetadata] {
    override def zero: PackageMetadata = PackageMetadata()
    override def append(f1: PackageMetadata, f2: => PackageMetadata): PackageMetadata =
      f1.append(f2)
  }
  implicit def equalPackageMetadata: Equal[PackageMetadata] =
    (a1: PackageMetadata, a2: PackageMetadata) => a1 == a2

  implicit def packageMetadataArbitrary: Arbitrary[PackageMetadata] = Arbitrary(packageMetadataGen)

  trait Scope {
    val template1 = Ref.Identifier.assertFromString("PackageName:ModuleName:template1")
    val template2 = Ref.Identifier.assertFromString("PackageName:ModuleName:template2")
    val iface1 = Ref.Identifier.assertFromString("PackageName:ModuleName:iface1")
    val iface2 = Ref.Identifier.assertFromString("PackageName:ModuleName:iface2")
    val view = PackageMetadataView.create
  }
}
