// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.SBuiltin._
import com.daml.lf.speedy.Anf.flattenToAnf
import com.daml.lf.speedy.Pretty.SExpr._
import com.daml.lf.data.Ref._

class AnfTest extends AnyWordSpec with Matchers {

  "identity: [\\x. x]" should {
    "be transformed to ANF as expected" in {
      val original = lam(1, arg0)
      val expected = original
      testTransform(original, expected)
    }
  }

  "twice: [\\f x. f (f x)]" should {
    "be transformed to ANF as expected" in {
      val original = lam(2, app(arg0, app(arg0, arg1)))
      val expected = lam(2, let1(appa(arg0, arg1), appa(arg0, stack1)))
      testTransform(original, expected)
    }
  }

  "thrice: [\\f x. f (f (f x))]" should {
    "be transformed to ANF as expected" in {
      val original = lam(2, app(arg0, app(arg0, app(arg0, arg1))))
      val expected =
        lam(2, let1(appa(arg0, arg1), let1(appa(arg0, stack1), appa(arg0, stack1))))
      testTransform(original, expected)
    }
  }

  "arithmetic non-atomic: [\\f x. f (x+1)]" should {
    "be transformed to ANF as expected" in {
      val original = lam(2, app(arg0, binop(SBAddInt64, arg1, num1)))
      val expected = lam(2, let1b2(SBAddInt64, arg1, num1, appa(arg0, stack1)))
      testTransform(original, expected)
    }
  }

  "nested (4x non-atomic): [\\f x. f(x+1) - f(x+2)]" should {
    "be transformed to ANF as expected" in {
      val original =
        lam(
          2,
          binop(
            SBSubInt64,
            app(arg0, binop(SBAddInt64, arg1, num1)),
            app(arg0, binop(SBAddInt64, arg1, num2))))
      val expected =
        lam(
          2,
          let1b2(
            SBAddInt64,
            arg1,
            num1,
            let1(
              appa(arg0, stack1),
              let1b2(
                SBAddInt64,
                arg1,
                num2,
                let1(appa(arg0, stack1), binopa(SBSubInt64, stack3, stack1))))))
      testTransform(original, expected)
    }
  }

  "builtin multi-arg fun: [\\g. (g 1) - (g 2)]" should {
    "be transformed to ANF as expected" in {
      val original =
        lam(1, binop(SBSubInt64, app(arg1, num1), app(arg1, num2)))
      val expected =
        lam(1, let1(appa(arg1, num1), let1(appa(arg1, num2), binopa(SBSubInt64, stack2, stack1))))
      testTransform(original, expected)
    }
  }

  "unknown multi-arg fun: [\\f g. f (g 1) (g 2)]" should {
    "be transformed to ANF as expected (safely)" in {
      val original =
        lam(2, app2(arg0, app(arg1, num1), app(arg1, num2)))
      val expected =
        lam(2, app2n(arg0, appa(arg1, num1), appa(arg1, num2)))
      testTransform(original, expected)
    }
  }

  "known apps nested in unknown: [\\f g x. f (g (x+1)) (g (x+2))]" should {
    "be transformed to ANF as expected (safely)" in {
      val original =
        lam(
          2,
          app2(
            arg0,
            app(arg1, binop(SBSubInt64, arg3, num1)),
            app(arg1, binop(SBSubInt64, arg3, num2))))
      val expected =
        lam(
          2,
          app2n(
            arg0,
            let1b2(SBSubInt64, arg3, num1, appa(arg1, stack1)),
            let1b2(SBSubInt64, arg3, num2, appa(arg1, stack1))))
      testTransform(original, expected)
    }
  }

  "error applied to 1 arg" should {
    "be transformed to ANF as expected" in {
      val original = lam(1, SEApp(SEBuiltin(SBError), Array(arg0)))
      val expected = lam(1, SEAppAtomicSaturatedBuiltin(SBError, Array(arg0)))
      testTransform(original, expected)
    }
  }

  "error (over) applied to 2 arg" should {
    "be transformed to ANF as expected" in {
      val original = lam(2, SEApp(SEBuiltin(SBError), Array(arg0, arg1)))
      val expected = lam(2, SEAppAtomicFun(SEBuiltin(SBError), Array(arg0, arg1)))
      testTransform(original, expected)
    }
  }

  "case expression: [\\a b c. if a then b else c]" should {
    "be transformed to ANF as expected" in {
      val original = lam(3, ite(arg0, arg1, arg2))
      val expected = lam(3, itea(arg0, arg1, arg2))
      testTransform(original, expected)
    }
  }

  "non-atomic in branch: [\\f x. if x==0 then 1 else f (div(1,x))]" should {
    "be transformed to ANF as expected" in {
      val original =
        lam(2, ite(binop(SBEqual, arg1, num0), num1, app(arg0, binop(SBDivInt64, num1, arg1))))
      val expected =
        lam(
          2,
          let1b2(
            SBEqual,
            arg1,
            num0,
            itea(stack1, num1, let1b2(SBDivInt64, num1, arg1, appa(arg0, stack1)))))
      testTransform(original, expected)
    }
  }

  "nested lambda: [\\f g. g (\\y. f (f y))]" should {
    "be transformed to ANF as expected" in {
      val original =
        lam(2, app(arg1, clo1(arg0, 1, app(free0, app(free0, arg0)))))
      val expected =
        lam(
          2,
          let1(clo1(arg0, 1, let1(appa(free0, arg0), appa(free0, stack1))), appa(arg1, stack1)))
      testTransform(original, expected)
    }
  }

  "issue 6535: [\\x. x + x]" should {
    "be transformed to ANF as expected (with no redundant lets)" in {
      val original = lam(1, binop(SBAddInt64, arg1, arg1))
      val expected = lam(1, binopa(SBAddInt64, arg1, arg1))
      testTransform(original, expected)
    }
  }

  // expression builders
  private def lam(n: Int, body: SExpr): SExpr = SEMakeClo(Array(), n, body)
  private def clo1(fv: SELoc, n: Int, body: SExpr): SExpr = SEMakeClo(Array(fv), n, body)

  private def app(func: SExpr, arg: SExpr): SExpr = SEAppGeneral(func, Array(arg))

  private def app2(func: SExpr, arg1: SExpr, arg2: SExpr): SExpr =
    SEAppGeneral(func, Array(arg1, arg2))

  private def app2n(func: SExprAtomic, arg1: SExpr, arg2: SExpr): SExpr =
    SEAppAtomicFun(func, Array(arg1, arg2))

  private def binop(op: SBuiltinPure, x: SExpr, y: SExpr): SExpr = SEApp(SEBuiltin(op), Array(x, y))

  private def ite(i: SExpr, t: SExpr, e: SExpr): SExpr =
    SECase(i, Array(SCaseAlt(patTrue, t), SCaseAlt(patFalse, e)))

  // anf builders
  private def let1(rhs: SExpr, body: SExpr): SExpr =
    SELet1General(rhs, body)

  private def let1b2(op: SBuiltinPure, arg1: SExprAtomic, arg2: SExprAtomic, body: SExpr): SExpr =
    SELet1Builtin(op, Array(arg1, arg2), body)

  private def appa(func: SExprAtomic, arg: SExprAtomic): SExpr =
    SEAppAtomicGeneral(func, Array(arg))

  private def binopa(op: SBuiltinPure, x: SExprAtomic, y: SExprAtomic): SExpr =
    SEAppAtomicSaturatedBuiltin(op, Array(x, y))

  private def itea(i: SExprAtomic, t: SExpr, e: SExpr): SExpr =
    SECaseAtomic(i, Array(SCaseAlt(patTrue, t), SCaseAlt(patFalse, e)))

  // true/false case-patterns
  private def patTrue: SCasePat =
    SCPVariant(Identifier.assertFromString("P:M:bool"), IdString.Name.assertFromString("True"), 1)

  private def patFalse: SCasePat =
    SCPVariant(Identifier.assertFromString("P:M:bool"), IdString.Name.assertFromString("False"), 2)

  // atoms

  private def arg0 = SELocA(0)
  private def arg1 = SELocA(1)
  private def arg2 = SELocA(2)
  private def arg3 = SELocA(3)
  private def free0 = SELocF(0)
  private def stack1 = SELocS(1)
  private def stack2 = SELocS(2)
  private def stack3 = SELocS(3)
  private def num0 = num(0)
  private def num1 = num(1)
  private def num2 = num(2)
  private def num(n: Long): SExprAtomic = SEValue(SInt64(n))

  // run a test...
  private def testTransform(original: SExpr, expected: SExpr, show: Boolean = false): Assertion = {
    val transformed = flattenToAnf(original)
    if (show || transformed != expected) {
      println(s"**original:\n${pp(original)}\n")
      println(s"**transformed:\n${pp(transformed)}\n")
      println(s"**expected:\n${pp(expected)}\n")
    }
    transformed shouldBe (expected)
  }

  private def pp(e: SExpr): String = {
    prettySExpr(0)(e).render(80)
  }

}
