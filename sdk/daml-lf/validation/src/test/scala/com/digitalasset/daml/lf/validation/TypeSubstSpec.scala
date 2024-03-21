// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.language.Ast._
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.validation.SpecUtil._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TypeSubstSpec extends AnyWordSpec with TableDrivenPropertyChecks with Matchers {

  "A TypeSubst" should {
    "should be idempotent on terms that do not contain variable from its domain." in {

      val subst = Map(n"alpha" -> t"gamma")

      val testCases = Table(
        "type",
        t"Int64",
        t"M:C",
        t"beta",
        t"gamma",
        t"beta1 beta2",
        t"beta1 -> beta2",
        t"< field1 : beta1, field2: beta2 >",
        t"M:C (beta1 beta2) ((beta2 -> beta1) beta2)",
      )

      forEvery(testCases) { typ: Type =>
        TypeSubst.substitute(subst, typ) shouldBe typ
      }
    }

    "should substitutes variables from its domain in terms without quantifiers." in {

      val subst1 = Map(n"alpha" -> t"gamma")
      val subst2 = Map(n"alpha" -> t"gamma2")

      val testCases = Table(
        "input type" ->
          "expect result type",
        t"alpha" ->
          t"gamma",
        t"alpha -> beta" ->
          t"gamma -> beta",
        t"beta -> alpha" ->
          t"beta -> gamma",
        t"alpha -> alpha" ->
          t"gamma -> gamma",
        t"alpha beta" ->
          t"gamma beta",
        t"beta alpha" ->
          t"beta gamma",
        t"alpha alpha" ->
          t"gamma gamma",
        t"<a: alpha, b: beta, c: alpha, d: beta>" ->
          t"<a: gamma, b: beta, c: gamma, d: beta>",
        t"M:C (alpha beta) ((beta -> alpha) beta)" ->
          t"M:C (gamma beta) ((beta -> gamma) beta)",
      )

      forEvery(testCases) { (input: Type, expectedOutput: Type) =>
        TypeSubst.substitute(subst1, input: Type) shouldEqual expectedOutput
        TypeSubst.substitute(subst2, input: Type) should !==(expectedOutput)
      }
    }

    "should handle properly binders" in {

      val subst = Map(n"alpha" -> t"beta1")

      TypeSubst.substitute(subst, t"forall beta1. alpha (beta1 gamma)") shouldEqual
        t"forall beta2. beta1 (beta2 gamma)"

      TypeSubst.substitute(subst, t"forall beta1. forall beta2. alpha (beta1 beta2)") shouldEqual
        t"forall gamma. forall beta2. beta1 (gamma beta2)"

      val subst0 = Map(n"beta2" -> t"beta1")
      val subst1 = Map(n"beta1" -> t"beta2")
      val input = t"forall beta1. forall beta2. alpha (beta1 beta2)"
      TypeSubst.substitute(subst1, TypeSubst.substitute(subst0, input)) shouldEqual input

    }
  }
}
