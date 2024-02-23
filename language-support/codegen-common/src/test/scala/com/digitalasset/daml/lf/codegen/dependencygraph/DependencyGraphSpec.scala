// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.dependencygraph

import com.daml.lf.codegen.Util
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.{Ref, ImmArray}
import com.daml.lf.typesig._
import PackageSignature.TypeDecl
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scalaz.syntax.std.map._

final class DependencyGraphSpec extends AnyWordSpec with Matchers {

  "orderedDependencies" should {
    "include contract keys" in {
      val declarations = Util.filterTemplatesBy(Seq("HasKey".r))(DependencyGraphSpec.typeDecls)
      DependencyGraph.orderedDependencies(declarations, Map.empty).deps map (_._1) should ===(
        Vector("a:b:It", "a:b:HasKey") map Ref.Identifier.assertFromString
      )
    }
    "include dependencies of interfaces" in {
      val interface = Ref.Identifier.assertFromString("a:b:Interface")
      val foo = Ref.Identifier.assertFromString("a:b:Foo")
      val bar = Ref.Identifier.assertFromString("a:b:Bar")
      val baz = Ref.Identifier.assertFromString("a:b:Baz")
      val quux = Ref.Identifier.assertFromString("a:b:Quux")
      val vt = Ref.Identifier.assertFromString("a:b:Vt")
      val minimalRecord =
        TypeDecl.Normal(DefDataType(ImmArraySeq.empty, Record(ImmArraySeq.empty)))
      DependencyGraph
        .orderedDependencies(
          serializableTypes = Map(
            foo -> TypeDecl.Normal(
              DefDataType(
                ImmArray(bar.qualifiedName.name.segments.last).toSeq,
                Record(ImmArraySeq.empty),
              )
            )
          ) ++ Iterable(bar, baz, quux, vt).view.map(_ -> minimalRecord),
          interfaces = Map(
            interface -> DefInterface(
              choices = Map(
                Ref.ChoiceName.assertFromString("SomeChoice") -> TemplateChoice[Type](
                  param = TypeCon(
                    TypeConName(foo),
                    ImmArray(TypeCon(TypeConName(bar), ImmArraySeq.empty)).toSeq,
                  ),
                  consuming = false,
                  returnType = TypeCon(TypeConName(baz), ImmArraySeq.empty),
                )
              ),
              viewType = Some(vt),
            )
          ),
        )
        .deps
        .map(_._1) should contain theSameElementsAs Vector(
        foo,
        bar,
        baz,
        interface,
        vt,
      )
    }
  }

}

object DependencyGraphSpec {

  private[this] val fooRec = Record(ImmArraySeq.empty)
  private val typeDecls =
    Map(
      "a:b:HasKey" -> TypeDecl.Template(
        fooRec,
        DefTemplate(
          TemplateChoices.Resolved(Map.empty),
          Some(TypeCon(TypeConName(Ref.Identifier assertFromString "a:b:It"), ImmArraySeq.empty)),
          Seq.empty,
        ),
      ),
      "a:b:NoKey" -> TypeDecl.Template(fooRec, DefTemplate.Empty),
      "a:b:It" -> TypeDecl.Normal(DefDataType(ImmArraySeq.empty, fooRec)),
    ) mapKeys Ref.Identifier.assertFromString

}
