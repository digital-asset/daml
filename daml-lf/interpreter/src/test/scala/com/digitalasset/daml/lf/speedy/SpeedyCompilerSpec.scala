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

    def compile0(expr: Expr): SExpr = compiler.compile(expr) // internal first step of compile
    //TODO: have step with just compile0 & closure conversion
    def compile1(expr: Expr): SExpr =
      compiler.unsafeCompile(expr) //compile0 + closure conversion + ANF

    "42" in {
      def before: Expr = EPrimLit(PLInt64(42))
      def expected0: SExpr = SEValue(SInt64(42))
      compile0(before) shouldBe expected0
      def expected1: SExpr = SEValue(SInt64(42))
      compile1(before) shouldBe expected1
    }

    def f: ExprVarName = Name.assertFromString("f")
    def x: ExprVarName = Name.assertFromString("x")

    def a: TypeVarName = Name.assertFromString("a") // use this for all types
    def ty: Type = TVar(a)

    "identity[ \\x -> x ]" in {
      def before: Expr = EAbs(binder = (x, ty), body = EVar(x), None)
      def expected0: SExpr = SEAbs(1, SEVar(1))
      compile0(before) shouldBe expected0
      def expected1: SExpr = SEMakeClo(Array(), 1, SELocA(0))
      compile1(before) shouldBe expected1
    }

    "apply[ \\f x -> f x ]" in {
      def before: Expr = EAbs((f, ty), EAbs((x, ty), EApp(EVar(f), EVar(x)), None), None)
      def expected0: SExpr = SEAbs(2, SEAppGeneral(SEVar(2), Array(SEVar(1))))
      compile0(before) shouldBe expected0
      def expected1: SExpr = SEMakeClo(Array(), 2, SEAppAtomicGeneral(SELocA(0), Array(SELocA(1))))
      compile1(before) shouldBe expected1
    }

    "twice[ \\f x -> f (f x) ]" in {
      def before: Expr =
        EAbs((f, ty), EAbs((x, ty), EApp(EVar(f), EApp(EVar(f), EVar(x))), None), None)
      def expected0: SExpr =
        SEAbs(2, SEAppGeneral(SEVar(2), Array(SEApp(SEVar(2), Array(SEVar(1))))))
      compile0(before) shouldBe expected0
      def expected1: SExpr = SEMakeClo(
        Array(),
        2,
        SELet1General(
          SEAppAtomicGeneral(SELocA(0), Array(SELocA(1))),
          SEAppAtomicGeneral(SELocA(0), Array(SELocS(1))),
        ),
      )
      compile1(before) shouldBe expected1
    }

  }
}
