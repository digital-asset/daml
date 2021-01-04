// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

import java.util

import com.daml.lf.PureCompiledPackages
import com.daml.lf.data
import com.daml.lf.language.Ast
import com.daml.lf.language.Ast.{Expr, Package}
import com.daml.lf.speedy.Compiler.FullStackTrace
import com.daml.lf.speedy.SResult.{SResultFinalValue}
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.validation.Validation
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TailCallTest extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  val pkg: Package =
    p"""
       module F {

         // *Non* tail-recursive definition
         val triangle : (Int64 -> Int64) = \ (x: Int64) ->
           case (EQUAL @Int64 x 0) of
               True -> 0
             | _    -> ADD_INT64 x (F:triangle (SUB_INT64 x 1));

         // Tail-recursive definition, via helper function
         val triangleTR : (Int64 -> Int64) = F:triangleTR_acc 0;

         // Tail-recursive definition using accumulator parameter
         val triangleTR_acc : (Int64 -> Int64 -> Int64) = \ (acc: Int64) (x: Int64) ->
           case (EQUAL @Int64 x 0) of
               True -> acc
             | _    -> F:triangleTR_acc (ADD_INT64 acc x) (SUB_INT64 x 1);


         val triangle_viaFoldLeft : (Int64 -> Int64) = \ (x: Int64) ->
            FOLDL @Int64 @Int64 ADD_INT64 0 (F:generate Nil@Int64 x);

         val triangle_viaFoldRight : (Int64 -> Int64) = \ (x: Int64) ->
            FOLDR @Int64 @Int64 ADD_INT64 0 (F:generate Nil@Int64 x);

         val triangle_viaFoldRight2 : (Int64 -> Int64) = \ (x: Int64) ->
            FOLDR @Int64 @Int64 (\(y: Int64) -> ADD_INT64 y) 0 (F:generate Nil@Int64 x);

         // tail-recursive generator
         val generate : (List Int64 -> Int64 -> List Int64) = \ (acc: List Int64) (x: Int64) ->
           case (EQUAL @Int64 x 0) of
               True -> acc
             | _    -> F:generate (Cons @Int64 [x] acc) (SUB_INT64 x 1);

       }
      """

  val small: Option[Int] = Some(5)
  val unbounded: Option[Int] = None

  "A *non* tail-recursive definition requires an unbounded env-stack, and an unbounded kont-stack" in {
    val exp = e"F:triangle 100"
    val expected = SValue.SInt64(5050)
    // The point of this test is to prove that the bounded-evaluation checking really works.
    runExpr(exp, envBound = unbounded, kontBound = unbounded) shouldBe expected

    the[RuntimeException]
      .thrownBy {
        runExpr(exp, envBound = small, kontBound = unbounded)
      }
      .toString() should include("BoundExceeded")

    the[RuntimeException]
      .thrownBy {
        runExpr(exp, envBound = unbounded, kontBound = small)
      }
      .toString() should include("BoundExceeded")
  }

  "A tail-recursive definition executes with a small env-stack, and a small kont-stack" in {
    val exp = e"F:triangleTR 100"
    val expected = SValue.SInt64(5050)
    runExpr(exp, envBound = small, kontBound = small) shouldBe expected
  }

  "fold-left executes with a small env-stack, and a small kont-stack" in {
    val exp = e"F:triangle_viaFoldLeft 100"
    val expected = SValue.SInt64(5050)
    runExpr(exp, envBound = small, kontBound = small) shouldBe expected
  }

  "fold-right executes with a small env-stack, and a small kont-stack" in {
    val exp = e"F:triangle_viaFoldRight 100"
    val expected = SValue.SInt64(5050)
    runExpr(exp, envBound = small, kontBound = small) shouldBe expected
  }

  "fold-right (KFoldr1Map/Reduce case) executes with a small env-stack, and a small kont-stack" in {
    val exp = e"F:triangle_viaFoldRight2 100"
    val expected = SValue.SInt64(5050)
    runExpr(exp, envBound = small, kontBound = small) shouldBe expected
  }

  private def typeAndCompile(pkg: Ast.Package): PureCompiledPackages = {
    val rawPkgs = Map(defaultParserParameters.defaultPackageId -> pkg)
    Validation.checkPackage(rawPkgs, defaultParserParameters.defaultPackageId, pkg)
    data.assertRight(
      PureCompiledPackages(rawPkgs, Compiler.Config.Default.copy(stacktracing = FullStackTrace)))
  }

  val pkgs = typeAndCompile(pkg)

  // Evaluate an expression with optionally bounded env and kont stacks
  private def runExpr(e: Expr, envBound: Option[Int], kontBound: Option[Int]): SValue = {
    // create the machine
    val machine = Speedy.Machine.fromPureExpr(pkgs, e)
    // maybe replace the env-stack with a bounded version
    envBound match {
      case None => ()
      case Some(bound) =>
        machine.env = new BoundedArrayList[SValue](bound)
    }
    // maybe replace the kont-stack with a bounded version
    kontBound match {
      case None => ()
      case Some(bound) =>
        val onlyKont: Speedy.Kont =
          if (machine.kontStack.size != 1) {
            crash(s"setBoundedKontStack, unexpected size of kont-stack: ${machine.kontStack.size}")
          } else {
            machine.kontStack.get(0)
          }
        machine.kontStack = new BoundedArrayList[Speedy.Kont](bound)
        machine.kontStack.add(onlyKont)
    }
    // run the machine
    machine.run() match {
      case SResultFinalValue(v) => v
      case res => crash(s"runExpr, unexpected result $res")
    }
  }

  private case object BoundExceeded extends RuntimeException

  private class BoundedArrayList[T](bound: Int) extends util.ArrayList[T](bound) {

    override def add(x: T): Boolean = {
      if (size >= bound) {
        throw BoundExceeded
      }
      super.add(x)
    }
  }

  def crash[A](reason: String): A = throw new RuntimeException(reason)

}
