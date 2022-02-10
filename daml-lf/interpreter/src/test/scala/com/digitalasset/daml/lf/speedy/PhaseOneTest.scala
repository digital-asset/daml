// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.SExpr0._
import com.daml.lf.language.PackageInterface

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.annotation.tailrec

class PhaseOneTest extends AnyFreeSpec with Matchers with TableDrivenPropertyChecks {

  // Construct one level of source-expression at various 'recursion-points'.
  private val app1 = (x: Expr) => EApp(x, leaf)
  private val app2 = (x: Expr) => EApp(leaf, x)

  "compilation phase #1 (stack-safety)" - {

    val phase1 = {
      def signatures: PartialFunction[PackageId, PackageSignature] = Map.empty
      def interface = new PackageInterface(signatures)
      def config =
        PhaseOne.Config(
          profiling = Compiler.NoProfile,
          stacktracing = Compiler.FullStackTrace,
        )
      new PhaseOne(interface, config)
    }

    // This is the code under test...
    def transform(e: Expr): SExpr = {
      phase1.translateFromLF(PhaseOne.Env.Empty, e)
    }

    /* We test stack-safety by building deep expressions through each of the different
     * recursion points of an expression, using one of the builder functions above, and
     * then ensuring we can 'transform' the expression using the phase1 compilation step.
     */
    def runTest(depth: Int, cons: Expr => Expr) = {
      // Make an expression by iterating the 'cons' function, 'depth' times
      @tailrec def loop(x: Expr, n: Int): Expr = if (n == 0) x else loop(cons(x), n - 1)
      val exp: Expr = loop(leaf, depth)
      val _: SExpr = transform(exp)
      true
    }

    // TODO https://github.com/digital-asset/daml/issues/11561
    // - add testcases for all Ast.Expr recursion points
    val testCases = {
      Table[String, Expr => Expr](
        ("name", "recursion-point"),
        ("app1", app1),
        ("app2", app2),
      )
    }

    {
      // TODO https://github.com/digital-asset/daml/issues/11561
      // -- tests should run to a depth of 10000 (currently this causes stack overflow)
      val depth = 100
      s"depth = $depth" - {
        forEvery(testCases) { (name: String, recursionPoint: Expr => Expr) =>
          name in {
            runTest(depth, recursionPoint)
          }
        }
      }
    }
  }

  private def leaf: Expr = EPrimLit(PLText("leaf"))

}
