// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.lf.language.PackageInterface
import com.daml.lf.speedy.Compiler.Config
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._

import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class SpeedyCompilerSpec extends AnyFreeSpec with Matchers with Inside {

  "speedy compile" - {

    def interface: PackageInterface = PackageInterface.Empty
    def config: Config = Config.Default
    def compiler: Compiler = new Compiler(interface, config)

    def compile(expr: Expr): SExpr = compiler.unsafeCompile(expr)

    "42" in {

      def before: Expr = EPrimLit(PLInt64(42))
      def expected: SExpr = SEValue(SInt64(42))
      compile(before) shouldBe expected
    }

    def x: ExprVarName = Name.assertFromString("x")
    def a: TypeVarName = Name.assertFromString("a")
    def ty: Type = TVar(a)

    "\\x -> x" in {

      def before: Expr = EAbs(binder = (x, ty), body = EVar(x), None)
      def expected: SExpr = SEMakeClo(Array(), 1, SELocA(0))
      compile(before) shouldBe expected
    }

  }

}
