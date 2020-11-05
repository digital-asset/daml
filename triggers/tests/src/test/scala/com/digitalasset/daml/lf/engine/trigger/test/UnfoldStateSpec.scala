// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger
package test

import akka.stream.ClosedShape
import akka.stream.scaladsl.{GraphDSL, Keep, RunnableGraph, Sink, Source}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import org.scalacheck.Gen.listOfN
import org.scalacheck.Arbitrary
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.Assertion
import org.scalatest.{AsyncWordSpec, Matchers}
import scalaz.{\/, -\/, \/-, Applicative, Monoid}
import scalaz.std.list._
import scalaz.std.scalaFuture._
import scalaz.syntax.apply.^
import scalaz.syntax.bifunctor._
import scalaz.syntax.traverse._

import scala.concurrent.Future

class UnfoldStateSpec
    extends AsyncWordSpec
    with Matchers
    with GeneratorDrivenPropertyChecks
    with AkkaBeforeAndAfterAll {
  import UnfoldState._

  "runTo" should {
    "retract fromLinearSeq" in forAll { xs: List[Int] =>
      fromLinearSeq(xs).runTo[List[Int]] should ===((xs, ()))
    }

    "retract fromIndexedSeq" in forAll { xs: Vector[Int] =>
      fromIndexedSeq(xs).runTo[Vector[Int]] should ===((xs, ()))
    }
  }

  "iterator" should {
    "retract fromLinearSeq and end with unit" in forAll { xs: List[Int] =>
      fromLinearSeq(xs).iterator().to[List] should ===((xs map \/.right) :+ -\/(()))
    }
  }

  "flatMapConcat" should {
    "do as built-in flatMapConcat would" in forAllFuture(trials = 10) { run: List[List[Int]] =>
      val flattened = run.flatten
      var escape = (0, 0)
      Source(run)
        .via(flatMapConcat(escape) { (sums, ns) =>
          UnfoldState((sums, ns)) {
            case ((sum, ct), hd +: tl) => \/-((hd, ((sum + hd, ct), tl)))
            case ((sum, ct), _) =>
              escape = (sum, ct + 1)
              -\/(escape)
          }
        })
        .runWith(Sink.seq)
        .map { ran =>
          ran should ===(flattened)
          escape should ===((flattened.sum, run.size))
        }
    }
  }

  "flatMapConcatStates" should {
    "emit every state after the list elements" in forAllFuture(trials = 10) {
      run: List[List[Int]] =>
        val (_, expected) = run.mapAccumL(0) { (sum, ns) =>
          val newSum = sum + ns.sum
          (newSum, (ns map \/.right) :+ -\/(newSum))
        }
        Source(run)
          .via(flatMapConcatStates(0) { (sum, ns) =>
            fromLinearSeq(ns) leftMap (_ => sum + ns.sum)
          })
          .runWith(Sink.seq)
          .map { ran =>
            ran should ===(expected.flatten)
          }
    }
  }

  "flatMapConcatNode" should {
    "emit same elements as flatMapConcat" in forAllFuture(trials = 10) { run: List[List[Int]] =>
      val (_, concatPairs) = run.mapAccumL(0) { (sum, ns) =>
        val newSum = sum + ns.sum
        (newSum, (ns, newSum))
      }
      val graph = GraphDSL.create(Sink.seq[Int], Sink.seq[Int])(Keep.both) {
        implicit gb => (nsOut, stOut) =>
          import GraphDSL.Implicits._
          val fmc = gb add flatMapConcatNode { (sum: Int, ns: List[Int]) =>
            fromLinearSeq(ns) leftMap (_ => sum + ns.sum)
          }
          fmc.initState <~ Source.single(0)
          fmc.elemsIn <~ Source(run)
          fmc.elemsOut ~> nsOut
          fmc.finalStates ~> stOut
          ClosedShape
      }
      val (fNs, fSt) = RunnableGraph.fromGraph(graph).run()
      ^(fNs, fSt) { (ns, st) =>
        st should ===(concatPairs map (_._2))
        ns should ===(concatPairs flatMap (_._1))
      }
    }
  }

  private[this] def forAllFuture[A](trials: Int)(f: A => Future[Assertion])(
      implicit A: Arbitrary[A]): Future[Assertion] = {
    val runs = listOfN(trials, A.arbitrary).sample
      .getOrElse(sys error "random Gen failed")

    implicit val assertionMonoid: Monoid[Future[Assertion]] =
      Monoid liftMonoid (Applicative[Future], Monoid instance ((_, result) => result, succeed))
    runs foldMap f
  }
}
