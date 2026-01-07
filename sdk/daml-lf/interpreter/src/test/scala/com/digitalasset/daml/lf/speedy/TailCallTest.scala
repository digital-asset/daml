// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.speedy.SResult.SResultFinal
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers

// TEST_EVIDENCE: Availability: Tail call optimization: Tail recursion does not blow the scala JVM stack.
class TailCallTest extends AnyFreeSpec with Matchers with TableDrivenPropertyChecks {

  import SpeedyTestLib.loggingContext

  implicit val defaultParserParameters: ParserParameters[this.type] = ParserParameters.default

  val pkgs = SpeedyTestLib.typeAndCompile(p"""
   metadata ( 'pkg' : '1.0.0' )

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
  """)

  // Evaluate an expression with specific cost model and gasBudget env and kont stacks
  def runExpr(e: Ast.Expr, gasBudget: Long, costModel: CostModel): SValue = {
    // create the machine
    val machine = Speedy.UpdateMachine(
      compiledPackages = pkgs,
      expr = pkgs.compiler.unsafeCompile(e),
      preparationTime = Time.Timestamp.Epoch,
      initialSeeding = InitialSeeding.NoSeed,
      committers = Set.empty,
      readAs = Set.empty,
      iterationsBetweenInterruptions = Long.MaxValue,
      packageResolution = Map.empty,
      costModel = costModel,
      initialGasBudget = Some(gasBudget),
      initialEnvSize = 1,
      initialKontStackSize = 1,
    )

    // run the machine
    machine.run() match {
      case SResultFinal(v) => v
      case res => crash(s"runExpr, unexpected result $res")
    }
  }

  "A *non* tail-recursive definition requires an unbounded env-stack, and an unbounded kont-stack" in {
    val exp = e"F:triangle 100"
    val expected = SValue.SInt64(5050)
    runExpr(exp, 0, costModelFreeStack) shouldBe expected

    the[RuntimeException]
      .thrownBy {
        runExpr(exp, 100, costModelFreeKonStack)
      }
      .toString() should include("No more gas")

    the[RuntimeException]
      .thrownBy {
        runExpr(exp, 100, costModelFreeEnv)
      }
      .toString() should include("No more gas")
  }

  "A tail-recursive definition executes with a small env-stack, and a small kont-stack" in {
    val exp = e"F:triangleTR 100"
    val expected = SValue.SInt64(5050)
    runExpr(exp, 12, costModelStack) shouldBe expected
  }

  "fold-left executes with a small env-stack, and a small kont-stack" in {
    val exp = e"F:triangle_viaFoldLeft 100"
    val expected = SValue.SInt64(5050)
    runExpr(exp, 12, costModelStack) shouldBe expected
  }

  "fold-right executes with a small env-stack, and a small kont-stack" in {
    val exp = e"F:triangle_viaFoldRight 100"
    val expected = SValue.SInt64(5050)
    runExpr(exp, 12, costModelStack) shouldBe expected
  }

  "fold-right (KFoldr1Map/Reduce case) executes with a small env-stack, and a small kont-stack" in {
    val exp = e"F:triangle_viaFoldRight2 100"
    val expected = SValue.SInt64(5050)
    runExpr(exp, 12, costModelStack) shouldBe expected
  }

  def crash[A](reason: String): A = throw new RuntimeException(reason)

  val costModelFreeStack = new CostModel.EmptyModel {}

  val costModelFreeEnv = new CostModel.EmptyModel {
    override val KontStackIncrease = _.toLong
  }
  val costModelFreeKonStack = new CostModel.EmptyModel {
    override val EnvIncrease = _.toLong
  }
  val costModelStack = new CostModel.EmptyModel {
    override val KontStackIncrease = _.toLong
    override val EnvIncrease = _.toLong
  }

}
