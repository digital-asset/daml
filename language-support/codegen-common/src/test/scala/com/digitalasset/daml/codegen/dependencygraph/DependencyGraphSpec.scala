// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen.dependencygraph

import com.daml.codegen.Util
import com.daml.lf.data.Ref
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scalaz.syntax.std.map._

final class DependencyGraphSpec extends AnyWordSpec with Matchers {

  "orderedDependencies" should {
    "include contract keys" in {
      val ei = Util.filterTemplatesBy(Seq("HasKey".r))(DependencyGraphSpec.envInterfaceWithKey)
      DependencyGraph.orderedDependencies(ei).deps map (_._1) should ===(
        Vector("a:b:It", "a:b:HasKey") map Ref.Identifier.assertFromString
      )
    }
  }

}

object DependencyGraphSpec {

  import com.daml.lf.iface._
  import com.daml.lf.data.ImmArray.ImmArraySeq

  private[this] val fooRec = Record(ImmArraySeq.empty)
  private val envInterfaceWithKey = EnvironmentInterface(
    Map.empty,
    Map(
      "a:b:HasKey" -> InterfaceType.Template(
        fooRec,
        DefTemplate(
          Map.empty,
          Map.empty,
          Some(TypeCon(TypeConName(Ref.Identifier assertFromString "a:b:It"), ImmArraySeq.empty)),
        ),
      ),
      "a:b:NoKey" -> InterfaceType.Template(fooRec, DefTemplate(Map.empty, Map.empty, None)),
      "a:b:It" -> InterfaceType.Normal(DefDataType(ImmArraySeq.empty, fooRec)),
    ) mapKeys Ref.Identifier.assertFromString,
    astInterfaces = Map.empty,
  )

}
