// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.daml.logging.LoggingContext
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.daml.lf.speedy.metrics.{StepCount, TxNodeCount}
import com.digitalasset.daml.lf.testing.parser.*
import org.openjdk.jmh.annotations.*

import java.util.concurrent.TimeUnit
import scala.collection.immutable.ArraySeq

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS) // Changed to microseconds here
class Bench {

  import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper

  private[this] implicit def logContext: LoggingContext = LoggingContext.ForTesting

  implicit def parserParameters: ParserParameters[this.type] =
    ParserParameters.default

  private[this] val metricPlugins = {
    val config = Engine.DevEngine.config

    Seq(new StepCount(config.iterationsBetweenInterruptions), new TxNodeCount)
  }

  private[this] def defaultPackageId = parserParameters.defaultPackageId

  private[this] def pkg =
    p"""
         metadata ( 'bench' : '1.0.0' )

         module Bench {

            val fib : Int64 -> Int64 = \(n: Int64) ->
              case (LESS @Int64 n 2) of
                True -> n
              | False -> ADD_INT64 (Bench:fib (SUB_INT64 n 1)) (Bench:fib (SUB_INT64 n 2)) ;

            val benchFib : Unit -> Int64 = \(_: Unit) -> Bench:fib 25;

            val longList: List Int64 = Bench:mkList 10000;

            val sumFoldlBuiltin: List Int64 -> Int64 = \(xs: List Int64) ->
              FOLDL @Int64 @Int64 ADD_INT64 0 xs;

            val benchSumFoldlBuiltin : Unit -> Int64 =
              \(_: Unit) -> Bench:sumFoldlBuiltin Bench:longList;

            val sumFoldrBuitlin: List Int64 -> Int64 = \(xs: List Int64) ->
              FOLDR @Int64 @Int64 ADD_INT64 0 xs;

            val benchSumFoldrBuiltin : Unit -> Int64 =
              \(_: Unit) -> Bench:sumFoldrBuitlin Bench:longList;

            val foldl: forall (a: *) (b: *). (a -> b -> a) -> a -> List b -> a = /\ (a: *) (b: *).
              \(f: a -> b -> a) (acc: a) (xs: List b) ->
                case xs of
                  Nil -> acc
                | Cons x xs -> Bench:foldl @a @b f (f acc x) xs;

            val foldr: forall (a: *) (b: *). (b -> a -> a) -> a -> List b -> a = /\ (a: *) (b: *).
              \(f: b -> a -> a) (acc: a) (xs: List b) ->
                case xs of
                  Nil -> acc
                | Cons x xs -> f x (Bench:foldr @a @b f acc xs);

            val sumFoldLCustom: List Int64 -> Int64 = \(xs: List Int64) ->
              Bench:foldl @Int64 @Int64 ADD_INT64 0 xs;

            val benchSumFoldlCustom : Unit -> Int64 =
              \(_: Unit) -> Bench:sumFoldLCustom Bench:longList;

            val sumFoldRCustom: List Int64 -> Int64 = \(xs: List Int64) ->
              Bench:foldr @Int64 @Int64 ADD_INT64 0 xs;

            val benchSumFoldrCustom : Unit -> Int64 =
              \(_: Unit) -> Bench:sumFoldRCustom Bench:longList;

            val mkList: Int64 -> List Int64 = \(i: Int64) ->
              case (LESS @Int64 0 i) of
                True -> Cons @Int64 [i] (Bench:mkList (SUB_INT64 i 1))
              | False -> Nil @Int64 ;

            val benchMkList : Unit -> List Int64 =
              \(_: Unit) -> Bench:mkList 10000;

            val mkListTail: Int64 -> List Int64 -> List Int64 = \(i: Int64) -> \(acc: List Int64) ->
             case (LESS @Int64 0 i) of
                True -> Bench:mkListTail (SUB_INT64 i 1) (Cons @Int64 [i] acc)
              | False -> acc ;

            val benchMkListTail : Unit -> List Int64 =
              \(_: Unit) -> Bench:mkListTail 10000 (Nil @Int64);

            val append: forall (a: *). List a -> List a -> List a = /\ (a: *).
              \(xs: List a) (ys: List a) ->
                case xs of
                  Nil -> ys
                | Cons x xs -> Cons @a [x] (Bench:append @a xs ys) ;

            val filter: forall (a: *). (a -> Bool) -> List a -> List a = /\ (a: *).
              \(p: a -> Bool) (xs: List a) ->
                 case xs of
                   Nil -> Nil @a
                 | Cons x xs ->
                     case p x of
                       True -> Cons @a [x] (Bench:filter @a p xs)
                     | False -> Bench:filter @a p xs ;

            val quicksort: forall (a: *). List a -> List a = /\ (a: *).
               \(xs: List a) ->
                 case xs of
                     Nil -> Nil @a
                 | Cons pivot xs ->
                     let smallerSorted: List a = Bench:quicksort @a (Bench:filter @a (GREATER @a pivot) xs) in
                     let biggerSorted: List a = Bench:quicksort @a (Bench:filter @a (LESS @a pivot) xs) in
                     Bench:append @a (Bench:append @a smallerSorted (Cons @a [pivot] (Nil @a))) biggerSorted ;

            val benchQuicksort : Unit -> List Int64 =
              \(_: Unit) -> Bench:quicksort @Int64 (Bench:longList);


         }
       """

  private[this] var compiledPackages: PureCompiledPackages = _
  private[this] var sexpr: SExpr.SExpr = _
  private var machine: Speedy.Machine[?] = _

  @Param(
    Array(
      "Bench:benchFib",
      "Bench:benchSumFoldlBuiltin",
      "Bench:benchSumFoldrBuiltin",
      "Bench:benchSumFoldlCustom",
      "Bench:benchSumFoldrCustom",
      "Bench:benchMkList",
      "Bench:benchMkListTail",
      "Bench:benchQuicksort",
    )
  )
  var b: String = null

  @Setup(Level.Trial)
  def init(): Unit = {
    val config = Compiler.Config.Dev
      .copy(packageValidation = Compiler.NoPackageValidation)
    compiledPackages = PureCompiledPackages.assertBuild(Map(defaultPackageId -> pkg), config)
    sexpr = SExpr.SEApp(compiledPackages.compiler.unsafeCompile(e"$b"), ArraySeq(SValue.SUnit))
  }

  @Benchmark
  def bench(counters: Bench.EventCounter): SValue = {
    counters.reset()
    machine = Speedy.Machine.fromPureSExpr(compiledPackages, sexpr, metricPlugins = metricPlugins)
    machine.setExpressionToEvaluate(sexpr)
    machine.run() match {
      case SResult.SResultFinal(v) =>
        counters.update(machine.metrics)
        v
      case otherwise =>
        throw new UnknownError(otherwise.toString)
    }
  }

}

object Bench {
  @State(Scope.Thread)
  @AuxCounters(AuxCounters.Type.EVENTS)
  class EventCounter {
    var stepCount: Long = 0
    var transactionNodeCount: Long = 0

    def reset(): Unit = {
      stepCount = 0
      transactionNodeCount = 0
    }

    def update(metrics: Speedy.Metrics): Unit = {
      stepCount += metrics.totalCount[StepCount].get
      transactionNodeCount += metrics.totalCount[TxNodeCount].get
    }
  }
}
