// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

import com.daml.lf.data.Ref

import com.daml.lf.speedy.Anf.flattenToAnf
import com.daml.lf.speedy.ClosureConversion.closureConvert
import com.daml.lf.speedy.{SExpr => expr}
import com.daml.lf.speedy.{SExpr0 => source}
import com.daml.lf.speedy.{SExpr1 => target}
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
  private val scopeExercise = (x: SExpr) => SEScopeExercise(x)
  private val preventCatch = (x: SExpr) => SEPreventCatch(x)
  private val labelClosure = (x: SExpr) => SELabelClosure(label, x)

  // This is the code under test...
  def transform1(e: SExpr): Boolean = {
    val _: target.SExpr = closureConvert(e)
    true
  }

  def transform2(e: SExpr): Boolean = {
    val e1: target.SExpr = closureConvert(e)
    val _ = flattenToAnf(e1)
    true
  }

  "stack-safety; deep" - {

    /* We test stack-safety by building deep expressions through each of the different
     * recursion points of an expression, using one of the builder functions above, and
     * then ensuring we can 'transform' the expression using 'closureConvert'.
     */
    def runTest(transform: SExpr => Boolean)(depth: Int, cons: SExpr => SExpr): Boolean = {
      // Make an expression by iterating the 'cons' function, 'depth' times
      @tailrec def loop(x: SExpr, n: Int): SExpr = if (n == 0) x else loop(cons(x), n - 1)
      val source: SExpr = loop(leaf, depth)
      transform(source)
    }

    val testCases = {
      Table[String, SExpr => SExpr](
        ("name", "recursion-point"),
        ("Abs", abs1),
        ("Location", location),
        ("AppF", appF),
        ("App1", app1),
        ("App2", app2),
        ("Scrut", scrut),
        ("Alt1", alt1),
        ("Alt2", alt2),
        ("Let1", let1),
        ("Let2", let2),
        ("LetBody", letBody),
        ("TryCatch1", tryCatch1),
        ("TryCatch2", tryCatch2),
        ("scopeExercise", scopeExercise),
        ("preventCatch", preventCatch),
        ("Labelclosure", labelClosure),
      )
    }

    {
      val depth = 10000
      s"transform(closureConversion), depth = $depth" - {
        forEvery(testCases) { (name: String, recursionPoint: SExpr => SExpr) =>
          name in {
            runTest(transform1)(depth, recursionPoint)
          }
        }
      }
    }

    {
      val depth = 10000
      s"transform(closureConversion,ANF), depth = $depth" - {
        forEvery(testCases) { (name: String, recursionPoint: SExpr => SExpr) =>
          name in {
            runTest(transform2)(depth, recursionPoint)
          }
        }
      }
    }

    "freeVars" - {
      def runTest(depth: Int, cons: SExpr => SExpr): Boolean = {
        // Make an expression by iterating the 'cons' function, 'depth' times..
        @tailrec def loop(x: SExpr, n: Int): SExpr = if (n == 0) x else loop(cons(x), n - 1)
        val exp: SExpr = abs1(loop(leaf, depth)) // ..embedded within a top-level abstraction..
        transform1(exp)
      }
      // ..to test stack-safety of the freeVars computation.
      {
        val depth = 10000
        val testCases = {
          Table[String, SExpr => SExpr](
            ("name", "recursion-point"),
            ("Location", location),
            ("Abs", abs1),
            ("AppF", appF),
            ("App1", app1),
            ("App2", app2),
            ("Scrut", scrut),
            ("Alt1", alt1),
            ("Alt2", alt2),
            ("Let1", let1),
            ("Let2", let2),
            ("LetBody", letBody),
            ("TryCatch1", tryCatch1),
            ("TryCatch2", tryCatch2),
            ("scopeExercise", scopeExercise),
            ("Labelclosure", labelClosure),
          )
        }
        s"depth = $depth" - {
          forEvery(testCases) { (name: String, recursionPoint: SExpr => SExpr) =>
            name in {
              runTest(depth, recursionPoint)
            }
          }
        }
      }
    }
  }

  "stack-safety; wide" - {

    val width = 10000

    val appGeneral = (xs: List[SExpr]) => {
      SEApp(leaf, xs)
    }
    val appWideBuiltin = (xs: List[SExpr]) => {
      SEApp(SEBuiltin(SBuiltin.SBConsMany(width)), xs)
    }
    val caseWide = (xs: List[SExpr]) => {
      SECase(leaf, xs.map(x => SCaseAlt(pat, x)))
    }
    val letWide = (xs: List[SExpr]) => {
      SELet(xs, leaf)
    }

    val testCases = {
      Table[String, List[SExpr] => SExpr](
        ("name", "recursion-point"),
        ("appGeneral", appGeneral),
        ("appWideBuiltin", appWideBuiltin),
        ("caseWide", caseWide),
        ("letWide", letWide),
      )
    }

    def runTest(transform: SExpr => Boolean)(width: Int, cons: List[SExpr] => SExpr): Boolean = {

      @tailrec def loop(x: SExpr, n: Int): SExpr = {
        if (n == 0) x
        else {
          val half = List.fill(width / 2)(leaf)
          val wide = half ++ List(x) ++ half
          loop(cons(wide), n - 1)
        }
      }
      val depth = 3
      val source: SExpr = loop(leaf, depth)
      transform(source)
    }

    {
      s"transform(closureConversion), width = $width" - {
        forEvery(testCases) { (name: String, recursionPoint: List[SExpr] => SExpr) =>
          name in {
            runTest(transform1)(width, recursionPoint)
          }
        }
      }
    }

    {
      s"transform(closureConversion,ANF), width = $width" - {
        forEvery(testCases) { (name: String, recursionPoint: List[SExpr] => SExpr) =>
          name in {
            runTest(transform2)(width, recursionPoint)
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
