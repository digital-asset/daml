// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.iface

import com.daml.scalatest.WordSpecCheckLaws
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scalaz.Equal
import scalaz.scalacheck.{ScalazProperties => SZP}

class DefDataTypeSpec extends AnyWordSpec with Matchers with WordSpecCheckLaws {
  import DefDataTypeSpec._
  "TemplateChoices Traverse" should {
    checkLaws(SZP.traverse.laws[TemplateChoices])
  }
}

object DefDataTypeSpec {
  import org.scalacheck.{Arbitrary, Gen}

  implicit def `TemplateChoices arb`[Ty]: Arbitrary[TemplateChoices[Ty]] =
    Arbitrary(Gen const TemplateChoices.Resolved(Map.empty)) // TODO SC actual gen

  implicit val `TemplateChoices eq`: Equal[TemplateChoices[Int]] = Equal.equalA
}
