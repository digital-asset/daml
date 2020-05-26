// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package codegen

import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref.Identifier
import com.daml.lf.iface._
import com.daml.lf.value.ValueGenerators.idGen

import org.scalatest.{WordSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class CodeGenSpec extends WordSpec with Matchers with GeneratorDrivenPropertyChecks {
  import CodeGen.filterTemplatesBy
  import CodeGenSpec._

  "filterTemplatesBy" should {
    "be identity given empty regexes" in forAll(trivialEnvInterfaceGen) { ei =>
      filterTemplatesBy(Seq.empty)(ei) should ===(ei)
    }

    "delete all templates given impossible regex" in forAll(trivialEnvInterfaceGen) { ei =>
      val noTemplates = ei copy (ei.typeDecls transform {
        case (_, tmpl @ InterfaceType.Template(_, _)) => InterfaceType.Normal(tmpl.`type`)
        case (_, v) => v
      })
      filterTemplatesBy(Seq("(?!a)a".r))(ei) should ===(noTemplates)
    }

    "match the union of regexes, not intersection" in forAll(trivialEnvInterfaceGen) { ei =>
      filterTemplatesBy(Seq("(?s).*".r, "(?!a)a".r))(ei) should ===(ei)
    }
  }
}

object CodeGenSpec {
  import org.scalacheck.{Arbitrary, Gen}
  import Arbitrary.arbitrary

  val trivialEnvInterfaceGen: Gen[EnvironmentInterface] = {
    val fooRec = Record(ImmArraySeq.empty)
    val fooTmpl = InterfaceType.Template(fooRec, DefTemplate(Map.empty, None))
    val fooNorm = InterfaceType.Normal(DefDataType(ImmArraySeq.empty, fooRec))
    implicit val idArb: Arbitrary[Identifier] = Arbitrary(idGen)
    arbitrary[Map[Identifier, Boolean]] map { ids =>
      EnvironmentInterface(ids transform { (_, isTemplate) =>
        if (isTemplate) fooTmpl else fooNorm
      })
    }
  }
}
