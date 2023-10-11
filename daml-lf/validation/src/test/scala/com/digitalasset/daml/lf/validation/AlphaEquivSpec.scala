// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.language.Ast.Type
import com.daml.lf.language.LanguageMajorVersion
import com.daml.lf.testing.parser.ParserParameters
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import com.daml.lf.testing.parser.Implicits.SyntaxHelper

class AlphaEquivSpecV1 extends AlphaEquivSpec(LanguageMajorVersion.V1)
class AlphaEquivSpecV2 extends AlphaEquivSpec(LanguageMajorVersion.V2)

class AlphaEquivSpec(majorLanguageVersion: LanguageMajorVersion)
    extends AnyWordSpec
    with TableDrivenPropertyChecks
    with Matchers {

  private[this] implicit val parserParameters: ParserParameters[this.type] =
    ParserParameters.defaultFor(majorLanguageVersion)

  import SpecUtil._

  "alphaEquiv" should {

    "is reflexive" in {

      val types = Table(
        "type",
        t"alpha",
        t"Int64",
        t"M:C",
        t"M:C alpha",
        t"alpha -> beta",
        t"forall beta1 . beta1 alpha",
        t"M:C (t1 t2) (t2 -> t1) t2",
      )

      forEvery(types) { typ: Type =>
        typ should ===(typ)
      }

    }

    "succeeds on term where bounds variables are renamed with non-free variables" in {

      val types1 = List(
        t"beta (forall beta1 . beta1 alpha)",
        t"beta (forall gamma . gamma alpha)",
        t"beta (forall beta . beta alpha)",
      )

      for (t1 <- types1; t2 <- types1) t1 should ===(t2)

      val types2 = List(
        t"beta (forall beta1 . beta1 -> forall beta2. beta1 beta2 alpha)",
        t"beta (forall beta2 . beta2 -> forall beta1. beta2 beta1 alpha)",
      )

      for (t1 <- types2; t2 <- types2) t1 should ===(t2)

      val types3 = List(
        t"beta (forall beta1 . beta1 -> (forall beta2. M:C beta2 alpha))",
        t"beta (forall beta2 . beta2 -> (forall beta1. M:C beta1 alpha))",
        t"beta (forall beta . beta -> (forall beta. M:C beta alpha))",
      )

      for (t1 <- types3; t2 <- types3) t1 should ===(t2)
    }

    "fails on terms where bounds variables are renamed with free variable" in {

      t"forall beta. beta alpha gamma" should !==(t"forall alpha . alpha alpha gamma")

      t"forall beta1. beta1 -> (forall beta2. beta1 beta2 alpha)" should !==(
        t"forall beta2. beta2 -> (forall beta2. beta2 beta2 alpha)"
      )

    }

  }

}
