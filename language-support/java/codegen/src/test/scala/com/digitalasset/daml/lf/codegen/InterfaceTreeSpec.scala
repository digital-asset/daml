// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen

import com.daml.lf.data.ImmArray
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref.{DottedName, QualifiedName, PackageId}
import com.daml.lf.iface.{DefDataType, Interface, InterfaceType, Record, Variant}
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable.ArrayBuffer

class InterfaceTreeSpec extends AnyFlatSpec with Matchers {

  behavior of "InterfaceTree.bfs"

  it should "traverse an empty tree" in {
    val interfaceTree =
      InterfaceTree(
        Map.empty,
        Interface(PackageId.assertFromString("packageid"), None, Map.empty, Map.empty),
      )
    interfaceTree.bfs(0)((x, _) => x + 1) shouldEqual 0
  }

  it should "traverse a tree with n elements in bfs order" in {
    val qualifiedName1 = QualifiedName(
      DottedName.assertFromSegments(ImmArray("foo").toSeq),
      DottedName.assertFromSegments(ImmArray("bar").toSeq),
    )
    val record1 = InterfaceType.Normal(DefDataType(ImmArraySeq(), Record(ImmArraySeq())))
    val qualifiedName2 =
      QualifiedName(
        DottedName.assertFromSegments(ImmArray("foo").toSeq),
        DottedName.assertFromSegments(ImmArray("bar", "baz").toSeq),
      )
    val variant1 = InterfaceType.Normal(DefDataType(ImmArraySeq(), Variant(ImmArraySeq())))
    val qualifiedName3 = QualifiedName(
      DottedName.assertFromSegments(ImmArray("foo").toSeq),
      DottedName.assertFromSegments(ImmArray("qux").toSeq),
    )
    val record2 = InterfaceType.Normal(DefDataType(ImmArraySeq(), Record(ImmArraySeq())))
    val typeDecls =
      Map(qualifiedName1 -> record1, qualifiedName2 -> variant1, qualifiedName3 -> record2)
    val interface = Interface(PackageId.assertFromString("packageId2"), None, typeDecls, Map.empty)
    val tree = InterfaceTree.fromInterface(interface)
    val result = tree.bfs(ArrayBuffer.empty[InterfaceType])((ab, n) =>
      n match {
        case ModuleWithContext(interface @ _, modulesLineage @ _, name @ _, module @ _) => ab
        case TypeWithContext(interface @ _, modulesLineage @ _, typesLineage @ _, name @ _, typ) =>
          ab ++= typ.typ.toList
      }
    )
    result should contain theSameElementsInOrderAs Seq(record1, record2, variant1)
  }

  behavior of "InterfaceTree.fromInterface"

  it should "permit standalone types with multi-component names" in {
    val bazQuux =
      QualifiedName(
        DottedName.assertFromSegments(ImmArray("foo", "bar").toSeq),
        DottedName.assertFromSegments(ImmArray("baz", "quux").toSeq),
      )
    val record = InterfaceType.Normal(DefDataType(ImmArraySeq(), Record(ImmArraySeq())))

    val typeDecls = Map(bazQuux -> record)
    val interface = Interface(PackageId.assertFromString("pkgid"), None, typeDecls, Map.empty)
    val tree = InterfaceTree.fromInterface(interface)
    val result = tree.bfs(ArrayBuffer.empty[InterfaceType])((types, n) =>
      n match {
        case _: ModuleWithContext => types
        case TypeWithContext(_, _, _, _, tpe) =>
          types ++= tpe.typ.toList
      }
    )
    result.toList shouldBe List(record)
  }

}
