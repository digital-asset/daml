package com.daml.lf.engine.trigger
package test

import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import org.scalacheck.Gen
import org.scalacheck.Arbitrary.arbitrary
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{AsyncWordSpec, Matchers}
import scalaz.{-\/, \/-}
import scalaz.std.list._
import scalaz.std.scalaFuture._
import scalaz.syntax.traverse._

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

  "flatMapConcat" should {
    "do as built-in flatMapConcat would" in {
      val trials = 10
      val runs = Gen
        .listOfN(trials, arbitrary[List[List[Int]]])
        .sample
        .getOrElse(sys error "random Gen failed")

      runs
        .traverse { run =>
          val flattened = run.flatten
          var escape = 0
          Source(run)
            .via(flatMapConcat(0) { (sum, ns) =>
              UnfoldState((sum, ns)) {
                case (sum, hd +: tl) => \/-((hd, (sum + hd, tl)))
                case (sum, _) =>
                  escape = sum
                  -\/(sum)
              }
            })
            .runWith(Sink.seq)
            .map { ran =>
              ran should ===(flattened)
              escape should ===(flattened.sum)
            }
        }
        .map(_.foldLeft(succeed)((_, result) => result))
    }
  }
}
