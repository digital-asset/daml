// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy
package compiler

import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import com.digitalasset.daml.lf.speedy.compiler.{SExpr1 => source}
import com.digitalasset.daml.lf.speedy.{SExpr => target}
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.speedy.SBuiltinFun._
import com.digitalasset.daml.lf.speedy.compiler.Anf.flattenToAnf
import com.digitalasset.daml.lf.speedy.Pretty.SExpr._
import com.digitalasset.daml.lf.data.Ref._

class AnfTest extends AnyWordSpec with Matchers {

  "identity: [\\x. x]" should {
    val original = slam(1, sarg0)
    val expected = lam(1, arg0)
    "be transformed to ANF as expected" in {
      testTransform(original, expected)
    }
  }

  "twice: [\\f x. f (f x)]" should {
    val original = slam(2, sapp(sarg0, sapp(sarg0, sarg1)))
    val expected = lam(2, let1(appa(arg0, arg1), appa(arg0, stack1)))
    "be transformed to ANF as expected" in {
      testTransform(original, expected)
    }
  }

  "thrice: [\\f x. f (f (f x))]" should {
    val original = slam(2, sapp(sarg0, sapp(sarg0, sapp(sarg0, sarg1))))
    val expected =
      lam(2, let1(appa(arg0, arg1), let1(appa(arg0, stack1), appa(arg0, stack1))))
    "be transformed to ANF as expected" in {
      testTransform(original, expected)
    }
  }

  "arithmetic non-atomic: [\\f x. f (x+1)]" should {
    val original = slam(2, sapp(sarg0, sbinop(SBAddInt64, sarg1, snum1)))
    val expected = lam(2, let1b2(SBAddInt64, arg1, num1, appa(arg0, stack1)))
    "be transformed to ANF as expected" in {
      testTransform(original, expected)
    }
  }

  "nested (4x non-atomic): [\\f x. f(x+1) - f(x+2)]" should {
    val original =
      slam(
        2,
        sbinop(
          SBSubInt64,
          sapp(sarg0, sbinop(SBAddInt64, sarg1, snum1)),
          sapp(sarg0, sbinop(SBAddInt64, sarg1, snum2)),
        ),
      )
    "be transformed to ANF as expected" in {
      val expected =
        lam(
          2,
          let1b2(
            SBAddInt64,
            arg1,
            num2,
            let1(
              appa(arg0, stack1),
              let1b2(
                SBAddInt64,
                arg1,
                num1,
                let1(appa(arg0, stack1), binopa(SBSubInt64, stack1, stack3)),
              ),
            ),
          ),
        )
      testTransform(original, expected)
    }
  }

  "builtin multi-arg fun: [\\g. (g 1) - (g 2)]" should {
    val original =
      slam(1, sbinop(SBSubInt64, sapp(sarg1, snum1), sapp(sarg1, snum2)))
    "be transformed to ANF as expected" in {
      val expected =
        lam(1, let1(appa(arg1, num2), let1(appa(arg1, num1), binopa(SBSubInt64, stack1, stack2))))
      testTransform(original, expected)
    }
  }

  "unknown multi-arg fun: [\\f g. f (g 1) (g 2)]" should {
    val original =
      slam(2, sapp2(sarg0, sapp(sarg1, snum1), sapp(sarg1, snum2)))
    "be transformed to ANF as expected (safely)" in {
      val expected =
        lam(2, let1(appa(arg1, num2), let1(appa(arg1, num1), appa(arg0, stack1, stack2))))
      testTransform(original, expected)
    }
  }

  "known apps nested in unknown: [\\f g x. f (g (x-1)) (g (x-2))]" should {
    val original =
      slam(
        2,
        sapp2(
          sarg0,
          sapp(sarg1, sbinop(SBSubInt64, sarg3, snum1)),
          sapp(sarg1, sbinop(SBSubInt64, sarg3, snum2)),
        ),
      )
    "be transformed to ANF as expected (safely)" in {
      val expected =
        lam(
          2,
          let1b2(
            SBSubInt64,
            arg3,
            num2,
            let1(
              appa(arg1, stack1),
              let1b2(SBSubInt64, arg3, num1, let1(appa(arg1, stack1), appa(arg0, stack1, stack3))),
            ),
          ),
        )
      testTransform(original, expected)
    }
  }

  "error applied to 1 arg" should {
    val original = slam(1, source.SEApp(source.SEBuiltin(SBUserError), List(sarg0)))
    val expected = lam(1, target.SEAppAtomicSaturatedBuiltin(SBUserError, Array(arg0)))
    "be transformed to ANF as expected" in {
      testTransform(original, expected)
    }
  }

  "error (over) applied to 2 arg" should {
    val original = slam(2, source.SEApp(source.SEBuiltin(SBUserError), List(sarg0, sarg1)))
    "be transformed to ANF as expected" in {
      val expected = lam(
        2,
        appa(target.SEBuiltinFun(SBUserError), arg0, arg1),
      )
      testTransform(original, expected)
    }
  }

  "case expression: [\\a b c. if a then b else c]" should {
    val original = slam(3, site(sarg0, sarg1, sarg2))
    val expected = lam(3, itea(arg0, arg1, arg2))
    "be transformed to ANF as expected" in {
      testTransform(original, expected)
    }
  }

  "non-atomic in branch: [\\f x. if x==0 then 1 else f (div(1,x))]" should {
    val original =
      slam(
        2,
        site(sbinop(SBEqual, sarg1, snum0), snum1, sapp(sarg0, sbinop(SBDivInt64, snum1, sarg1))),
      )
    "be transformed to ANF as expected" in {
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
    val original =
      slam(2, sapp(sarg1, sclo1(sarg0, 1, sapp(sfree0, sapp(sfree0, sarg0)))))
    val expected =
      lam(
        2,
        let1(clo1(arg0, 1, let1(appa(free0, arg0), appa(free0, stack1))), appa(arg1, stack1)),
      )
    "be transformed to ANF as expected" in {
      testTransform(original, expected)
    }
  }

  "issue 6535: [\\x. x + x]" should {
    val original = slam(1, sbinop(SBAddInt64, sarg1, sarg1))
    val expected = lam(1, binopa(SBAddInt64, arg1, arg1))
    "be transformed to ANF as expected (with no redundant lets)" in {
      testTransform(original, expected)
    }
  }

  // expression builders
  private def lam(n: Int, body: target.SExpr): target.SExpr = target.SEMakeClo(Array(), n, body)
  private def clo1(fv: target.SELoc, n: Int, body: target.SExpr): target.SExpr =
    target.SEMakeClo(Array(fv), n, body)

  // anf builders
  private def let1(rhs: target.SExpr, body: target.SExpr): target.SExpr =
    target.SELet1General(rhs, body)

  private def let1b2(
      op: SBuiltinPure,
      arg1: target.SExprAtomic,
      arg2: target.SExprAtomic,
      body: target.SExpr,
  ): target.SExpr =
    target.SELet1Builtin(op, Array(arg1, arg2), body)

  private def let1b2(
      op: SBuiltinArithmetic,
      arg1: target.SExprAtomic,
      arg2: target.SExprAtomic,
      body: target.SExpr,
  ): target.SExpr =
    target.SELet1BuiltinArithmetic(op, Array(arg1, arg2), body)

  private def appa(func: target.SExprAtomic, args: target.SExprAtomic*): target.SExpr =
    target.SEAppAtomicGeneral(func, args.toArray)

  private def binopa(
      op: SBuiltinArithmetic,
      x: target.SExprAtomic,
      y: target.SExprAtomic,
  ): target.SExpr =
    target.SEAppAtomicSaturatedBuiltin(op, Array(x, y))

  private def itea(i: target.SExprAtomic, th: target.SExpr, e: target.SExpr): target.SExpr =
    target.SECaseAtomic(i, Array(target.SCaseAlt(patTrue, th), target.SCaseAlt(patFalse, e)))

  // true/false case-patterns
  private def patTrue: target.SCasePat =
    target.SCPVariant(
      Identifier.assertFromString("P:M:bool"),
      IdString.Name.assertFromString("True"),
      1,
    )

  private def patFalse: target.SCasePat =
    target.SCPVariant(
      Identifier.assertFromString("P:M:bool"),
      IdString.Name.assertFromString("False"),
      2,
    )

  // atoms

  private def arg0 = target.SELocA(0)
  private def arg1 = target.SELocA(1)
  private def arg2 = target.SELocA(2)
  private def arg3 = target.SELocA(3)
  private def free0 = target.SELocF(0)
  private def stack1 = target.SELocS(1)
  private def stack2 = target.SELocS(2)
  private def stack3 = target.SELocS(3)
  private def num0 = num(0)
  private def num1 = num(1)
  private def num2 = num(2)
  private def num(n: Long): target.SExprAtomic = target.SEValue(SInt64(n))

  // We have different expression types before/after the ANF transform, so we different constructors.
  // Use "s" (for "source") as a prefix to distinguish.
  private def slam(n: Int, body: source.SExpr): source.SExpr = source.SEMakeClo(List(), n, body)
  private def sclo1(fv: source.SELoc, n: Int, body: source.SExpr): source.SExpr =
    source.SEMakeClo(List(fv), n, body)
  private def sapp(func: source.SExpr, arg: source.SExpr): source.SExpr =
    source.SEApp(func, List(arg))
  private def sbinop(op: SBuiltinPure, x: source.SExpr, y: source.SExpr): source.SExpr =
    source.SEApp(source.SEBuiltin(op), List(x, y))
  private def sbinop(op: SBuiltinArithmetic, x: source.SExpr, y: source.SExpr): source.SExpr =
    source.SEApp(source.SEBuiltin(op), List(x, y))
  private def sapp2(func: source.SExpr, arg1: source.SExpr, arg2: source.SExpr): source.SExpr =
    source.SEApp(func, List(arg1, arg2))
  private def site(i: source.SExpr, t: source.SExpr, e: source.SExpr): source.SExpr =
    source.SECase(i, List(source.SCaseAlt(patTrue, t), source.SCaseAlt(patFalse, e)))
  private def sarg0 = source.SELocA(0)
  private def sarg1 = source.SELocA(1)
  private def sarg2 = source.SELocA(2)
  private def sarg3 = source.SELocA(3)
  private def sfree0 = source.SELocF(0)
  private def snum0 = snum(0)
  private def snum1 = snum(1)
  private def snum2 = snum(2)
  private def snum(n: Long): source.SExprAtomic = source.SEValue(SInt64(n))

  // run a test...
  private def testTransform(
      original: source.SExpr,
      expected: target.SExpr,
      show: Boolean = false,
  ): Assertion = {
    val transformed = flattenToAnf(original)
    if (show || transformed != expected) {
      println(s"**original:\n${original}\n")
      println(s"**transformed:\n${pp(transformed)}\n")
      println(s"**expected:\n${pp(expected)}\n")
    }
    transformed shouldBe (expected)
  }

  private def pp(e: target.SExpr): String = {
    prettySExpr(0)(e).render(80)
  }

}
