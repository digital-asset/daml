// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

import com.daml.lf.data.Ref

import com.daml.lf.speedy.{SExpr0 => source}
import com.daml.lf.speedy.{SExpr1 => target}
import com.daml.lf.speedy.{SExpr => expr}
import com.daml.lf.speedy.{SValue => v}

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.annotation.tailrec

class ClosureConversionTest extends AnyFreeSpec with Matchers with TableDrivenPropertyChecks {

  import source._

  // Construct one level of source-expression at various 'recursion-points'.
  // This list is intended to be exhaustive.
  private val location = (x: SExpr) => SELocation(loc, x)
  private val abs1 = (x: SExpr) => SEAbs(1, x)
  private val appF = (x: SExpr) => SEApp(x, List(leaf, leaf))
  private val app1 = (x: SExpr) => SEApp(leaf, List(x, leaf))
  private val app2 = (x: SExpr) => SEApp(leaf, List(leaf, x))
  private val scrut = (x: SExpr) => SECase(x, List(SCaseAlt(pat, leaf), SCaseAlt(pat, leaf)))
  private val alt1 = (x: SExpr) => SECase(leaf, List(SCaseAlt(pat, x), SCaseAlt(pat, leaf)))
  private val alt2 = (x: SExpr) => SECase(leaf, List(SCaseAlt(pat, leaf), SCaseAlt(pat, x)))
  private val let1 = (x: SExpr) => SELet(List(x, leaf), leaf)
  private val let2 = (x: SExpr) => SELet(List(leaf, x), leaf)
  private val letBody = (x: SExpr) => SELet(List(leaf, leaf), x)
  private val tryCatch1 = (x: SExpr) => SETryCatch(leaf, x)
  private val tryCatch2 = (x: SExpr) => SETryCatch(x, leaf)
  private val labelClosure = (x: SExpr) => SELabelClosure(label, x)

  "closure conversion" - {

    // This is the code under test...
    def transform(e: SExpr): target.SExpr = {
      import com.daml.lf.speedy.ClosureConversion.closureConvert
      closureConvert(e)
    }

    /* We test stack-safety by building deep expressions through each of the different
     * recursion points of an expression, using one of the builder functions above, and
     * then ensuring we can 'transform' the expression using 'closureConvert'.
     */
    def runTest(depth: Int, cons: SExpr => SExpr) = {
      // Make an expression by iterating the 'cons' function, 'depth' times
      @tailrec def loop(x: SExpr, n: Int): SExpr = if (n == 0) x else loop(cons(x), n - 1)
      val exp: SExpr = loop(leaf, depth)
      val _: target.SExpr = transform(exp)
      true
    }

    /* The testcases are split into two sets:
     *
     * For both sets the code under test is stack-safe, but the 2nd set provokes an
     * unrelated quadratic-or-worse time-issue in the handling of 'Env' management and the
     * free-vars computation, during the closure-conversion transform.
     */
    val testCases1 = {
      Table[String, SExpr => SExpr](
        ("name", "recursion-point"),
        ("Location", location),
        ("AppF", appF),
        ("App1", app1),
        ("App2", app2),
        ("Scrut", scrut),
        ("Let1", let1),
        ("TryCatch2", tryCatch2),
        ("Labelclosure", labelClosure),
        ("Alt1", alt1),
        ("Alt2", alt2),
        ("Let2", let2),
        ("LetBody", letBody),
        ("TryCatch1", tryCatch1),
      )
    }

    // These 'quadratic' testcases pertain to recursion-points under a binder.
    val testCases2 = {
      Table[String, SExpr => SExpr](
        ("name", "recursion-point"),
        ("Abs", abs1),
      )
    }

    {
      val depth = 100000
      s"depth = $depth" - {
        forEvery(testCases1) { (name: String, recursionPoint: SExpr => SExpr) =>
          name in {
            runTest(depth, recursionPoint)
          }
        }
      }
    }

    {
      // TODO: There remains a quadratic issue with the freeVars calculation (#11830).
      // This affects only Abs testcase. It takes 12s when run to a larger depth of 100k.
      // So we only run to 10k.
      val depth = 10000
      s"depth = $depth" - {
        forEvery(testCases2) { (name: String, recursionPoint: SExpr => SExpr) =>
          name in {
            runTest(depth, recursionPoint)
          }
        }
      }
    }
  }

  private val leaf = SEValue(v.SText("leaf"))
  private val label: Profile.Label = expr.AnonymousClosure
  private val pat: expr.SCasePat = expr.SCPCons
  private val loc = Ref.Location(
    Ref.PackageId.assertFromString("P"),
    Ref.ModuleName.assertFromString("M"),
    "X",
    (1, 2),
    (3, 4),
  )
}
