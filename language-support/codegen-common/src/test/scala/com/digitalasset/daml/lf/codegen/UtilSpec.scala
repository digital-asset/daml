// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package codegen

import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref.Identifier
import com.daml.lf.iface._
import com.daml.lf.value.test.ValueGenerators.idGen

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class UtilSpec extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {
  import Util.filterTemplatesBy
  import UtilSpec._

  "filterTemplatesBy" should {
    "be identity given empty regexes" in forAll(trivialDeclarations) { declarations =>
      filterTemplatesBy(Seq.empty)(declarations) should ===(declarations)
    }

    "delete all templates given impossible regex" in forAll(trivialDeclarations) { declarations =>
      val noTemplates = declarations transform {
        case (_, tmpl @ InterfaceType.Template(_, _)) => InterfaceType.Normal(tmpl.`type`)
        case (_, v) => v
      }
      filterTemplatesBy(Seq("(?!a)a".r))(declarations) should ===(noTemplates)
    }

    "match the union of regexes, not intersection" in forAll(trivialDeclarations) { declarations =>
      filterTemplatesBy(Seq("(?s).*".r, "(?!a)a".r))(declarations) should ===(declarations)
    }
  }
}

object UtilSpec {
  import org.scalacheck.{Arbitrary, Gen}
  import Arbitrary.arbitrary

  val trivialDeclarations: Gen[Map[Identifier, InterfaceType]] = {
    val fooRec = Record(ImmArraySeq.empty)
    val fooTmpl = InterfaceType.Template(fooRec, DefTemplate.Empty)
    val fooNorm = InterfaceType.Normal(DefDataType(ImmArraySeq.empty, fooRec))
    implicit val idArb: Arbitrary[Identifier] = Arbitrary(idGen)
    arbitrary[Map[Identifier, Boolean]] map {
      _ transform { (_, isTemplate) =>
        if (isTemplate) fooTmpl else fooNorm
      }
    }
  }
}
