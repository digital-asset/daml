// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import com.daml.lf.speedy.{SExpr0 => s}
import com.daml.lf.speedy.{SExpr => t}
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.SBuiltin._
import com.daml.lf.speedy.Anf.flattenToAnf
import com.daml.lf.speedy.Pretty.SExpr._
import com.daml.lf.data.Ref._

class AnfTest extends AnyWordSpec with Matchers {

  "identity: [\\x. x]" should {
    "be transformed to ANF as expected" in {
      val original = slam(1, sarg0)
      val expected = lam(1, arg0)
      testTransform(original, expected)
    }
  }

  "twice: [\\f x. f (f x)]" should {
    "be transformed to ANF as expected" in {
      val original = slam(2, sapp(sarg0, sapp(sarg0, sarg1)))
      val expected = lam(2, let1(appa(arg0, arg1), appa(arg0, stack1)))
      testTransform(original, expected)
    }
  }

  "thrice: [\\f x. f (f (f x))]" should {
    "be transformed to ANF as expected" in {
      val original = slam(2, sapp(sarg0, sapp(sarg0, sapp(sarg0, sarg1))))
      val expected =
        lam(2, let1(appa(arg0, arg1), let1(appa(arg0, stack1), appa(arg0, stack1))))
      testTransform(original, expected)
    }
  }

  "arithmetic non-atomic: [\\f x. f (x+1)]" should {
    "be transformed to ANF as expected" in {
      val original = slam(2, sapp(sarg0, sbinop(SBAddInt64, sarg1, snum1)))
      val expected = lam(2, let1b2(SBAddInt64, arg1, num1, appa(arg0, stack1)))
      testTransform(original, expected)
    }
  }

  "nested (4x non-atomic): [\\f x. f(x+1) - f(x+2)]" should {
    "be transformed to ANF as expected" in {
      val original =
        slam(
          2,
          sbinop(
            SBSubInt64,
            sapp(sarg0, sbinop(SBAddInt64, sarg1, snum1)),
            sapp(sarg0, sbinop(SBAddInt64, sarg1, snum2)),
          ),
        )
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
                let1(appa(arg0, stack1), binopa(SBSubInt64, stack3, stack1)),
              ),
            ),
          ),
        )
      testTransform(original, expected)
    }
  }

  "builtin multi-arg fun: [\\g. (g 1) - (g 2)]" should {
    "be transformed to ANF as expected" in {
      val original =
        slam(1, sbinop(SBSubInt64, sapp(sarg1, snum1), sapp(sarg1, snum2)))
      val expected =
        lam(1, let1(appa(arg1, num1), let1(appa(arg1, num2), binopa(SBSubInt64, stack2, stack1))))
      testTransform(original, expected)
    }
  }

  "unknown multi-arg fun: [\\f g. f (g 1) (g 2)]" should {
    "be transformed to ANF as expected (safely)" in {
      val original =
        slam(2, sapp2(sarg0, sapp(sarg1, snum1), sapp(sarg1, snum2)))
      val expected =
        lam(2, app2n(arg0, appa(arg1, num1), appa(arg1, num2)))
      testTransform(original, expected)
    }
  }

  "known apps nested in unknown: [\\f g x. f (g (x+1)) (g (x+2))]" should {
    "be transformed to ANF as expected (safely)" in {
      val original =
        slam(
          2,
          sapp2(
            sarg0,
            sapp(sarg1, sbinop(SBSubInt64, sarg3, snum1)),
            sapp(sarg1, sbinop(SBSubInt64, sarg3, snum2)),
          ),
        )
      val expected =
        lam(
          2,
          app2n(
            arg0,
            let1b2(SBSubInt64, arg3, num1, appa(arg1, stack1)),
            let1b2(SBSubInt64, arg3, num2, appa(arg1, stack1)),
          ),
        )
      testTransform(original, expected)
    }
  }

  "error applied to 1 arg" should {
    "be transformed to ANF as expected" in {
      val original = slam(1, s.SEApp(s.SEBuiltin(SBError), Array(sarg0)))
      val expected = lam(1, t.SEAppAtomicSaturatedBuiltin(SBError, Array(arg0)))
      testTransform(original, expected)
    }
  }

  "error (over) applied to 2 arg" should {
    "be transformed to ANF as expected" in {
      val original = slam(2, s.SEApp(s.SEBuiltin(SBError), Array(sarg0, sarg1)))
      val expected = lam(2, t.SEAppAtomicFun(t.SEBuiltin(SBError), Array(arg0, arg1)))
      testTransform(original, expected)
    }
  }

  "case expression: [\\a b c. if a then b else c]" should {
    "be transformed to ANF as expected" in {
      val original = slam(3, site(sarg0, sarg1, sarg2))
      val expected = lam(3, itea(arg0, arg1, arg2))
      testTransform(original, expected)
    }
  }

  "non-atomic in branch: [\\f x. if x==0 then 1 else f (div(1,x))]" should {
    "be transformed to ANF as expected" in {
      val original =
        slam(
          2,
          site(sbinop(SBEqual, sarg1, snum0), snum1, sapp(sarg0, sbinop(SBDivInt64, snum1, sarg1))),
        )
      val expected =
        lam(
          2,
          let1b2(
            SBEqual,
            arg1,
            num0,
            itea(stack1, num1, let1b2(SBDivInt64, num1, arg1, appa(arg0, stack1))),
          ),
        )
      testTransform(original, expected)
    }
  }

  "nested lambda: [\\f g. g (\\y. f (f y))]" should {
    "be transformed to ANF as expected" in {
      val original =
        slam(2, sapp(sarg1, sclo1(sarg0, 1, sapp(sfree0, sapp(sfree0, sarg0)))))
      val expected =
        lam(
          2,
          let1(clo1(arg0, 1, let1(appa(free0, arg0), appa(free0, stack1))), appa(arg1, stack1)),
        )
      testTransform(original, expected)
    }
  }

  "issue 6535: [\\x. x + x]" should {
    "be transformed to ANF as expected (with no redundant lets)" in {
      val original = slam(1, sbinop(SBAddInt64, sarg1, sarg1))
      val expected = lam(1, binopa(SBAddInt64, arg1, arg1))
      testTransform(original, expected)
    }
  }

  // expression builders
  private def lam(n: Int, body: t.SExpr): t.SExpr = t.SEMakeClo(Array(), n, body)
  private def clo1(fv: t.SELoc, n: Int, body: t.SExpr): t.SExpr = t.SEMakeClo(Array(fv), n, body)

  private def app2n(func: t.SExprAtomic, arg1: t.SExpr, arg2: t.SExpr): t.SExpr =
    t.SEAppAtomicFun(func, Array(arg1, arg2))

  // anf builders
  private def let1(rhs: t.SExpr, body: t.SExpr): t.SExpr =
    t.SELet1General(rhs, body)

  private def let1b2(
      op: SBuiltinPure,
      arg1: t.SExprAtomic,
      arg2: t.SExprAtomic,
      body: t.SExpr,
  ): t.SExpr =
    t.SELet1Builtin(op, Array(arg1, arg2), body)

  private def let1b2(
      op: SBuiltinArithmetic,
      arg1: t.SExprAtomic,
      arg2: t.SExprAtomic,
      body: t.SExpr,
  ): t.SExpr =
    t.SELet1BuiltinArithmetic(op, Array(arg1, arg2), body)

  private def appa(func: t.SExprAtomic, arg: t.SExprAtomic): t.SExpr =
    t.SEAppAtomicGeneral(func, Array(arg))

  private def binopa(op: SBuiltinArithmetic, x: t.SExprAtomic, y: t.SExprAtomic): t.SExpr =
    t.SEAppAtomicSaturatedBuiltin(op, Array(x, y))

  private def itea(i: t.SExprAtomic, th: t.SExpr, e: t.SExpr): t.SExpr =
    t.SECaseAtomic(i, Array(t.SCaseAlt(patTrue, th), t.SCaseAlt(patFalse, e)))

  // true/false case-patterns
  private def patTrue: t.SCasePat =
    t.SCPVariant(Identifier.assertFromString("P:M:bool"), IdString.Name.assertFromString("True"), 1)

  private def patFalse: t.SCasePat =
    t.SCPVariant(
      Identifier.assertFromString("P:M:bool"),
      IdString.Name.assertFromString("False"),
      2,
    )

  // atoms

  private def arg0 = t.SELocA(0)
  private def arg1 = t.SELocA(1)
  private def arg2 = t.SELocA(2)
  private def arg3 = t.SELocA(3)
  private def free0 = t.SELocF(0)
  private def stack1 = t.SELocS(1)
  private def stack2 = t.SELocS(2)
  private def stack3 = t.SELocS(3)
  private def num0 = num(0)
  private def num1 = num(1)
  private def num2 = num(2)
  private def num(n: Long): t.SExprAtomic = t.SEValue(SInt64(n))

  // We have different expression types before/after the ANF transform, so we different constructors.
  // Use "s" (for "source") as a prefix to distinguish.
  private def slam(n: Int, body: s.SExpr): s.SExpr = s.SEMakeClo(Array(), n, body)
  private def sclo1(fv: s.SELoc, n: Int, body: s.SExpr): s.SExpr = s.SEMakeClo(Array(fv), n, body)
  private def sapp(func: s.SExpr, arg: s.SExpr): s.SExpr = s.SEAppGeneral(func, Array(arg))
  private def sbinop(op: SBuiltinPure, x: s.SExpr, y: s.SExpr): s.SExpr =
    s.SEApp(s.SEBuiltin(op), Array(x, y))
  private def sbinop(op: SBuiltinArithmetic, x: s.SExpr, y: s.SExpr): s.SExpr =
    s.SEApp(s.SEBuiltin(op), Array(x, y))
  private def sapp2(func: s.SExpr, arg1: s.SExpr, arg2: s.SExpr): s.SExpr =
    s.SEAppGeneral(func, Array(arg1, arg2))
  private def site(i: s.SExpr, t: s.SExpr, e: s.SExpr): s.SExpr =
    s.SECase(i, Array(s.SCaseAlt(patTrue, t), s.SCaseAlt(patFalse, e)))
  private def sarg0 = s.SELocA(0)
  private def sarg1 = s.SELocA(1)
  private def sarg2 = s.SELocA(2)
  private def sarg3 = s.SELocA(3)
  private def sfree0 = s.SELocF(0)
  private def snum0 = snum(0)
  private def snum1 = snum(1)
  private def snum2 = snum(2)
  private def snum(n: Long): s.SExprAtomic = s.SEValue(SInt64(n))

  // run a test...
  private def testTransform(
      original: s.SExpr,
      expected: t.SExpr,
      show: Boolean = false,
  ): Assertion = {
    val transformed = flattenToAnf(original)
    if (show || transformed != expected) {
      //println(s"**original:\n${pp(original)}\n")
      println(s"**transformed:\n${pp(transformed)}\n")
      println(s"**expected:\n${pp(expected)}\n")
    }
    transformed shouldBe (expected)
  }

  private def pp(e: t.SExpr): String = {
    prettySExpr(0)(e).render(80)
  }

}
