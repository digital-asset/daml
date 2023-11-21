// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.util

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import org.scalacheck.{Gen, Arbitrary}
import org.scalatest.concurrent.ScalaFutures
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scalaz.{-\/, \/, \/-}

import scala.concurrent.Future
import scala.concurrent.duration._

class FlowUtilTest
    extends AnyFlatSpec
    with ScalaFutures
    with Matchers
    with ScalaCheckDrivenPropertyChecks {
  import com.digitalasset.canton.http.util.FlowUtil._

  implicit val asys: ActorSystem = ActorSystem(this.getClass.getSimpleName)
  implicit val materializer: Materializer = Materializer(asys)

  "allowOnlyFirstInput" should "pass 1st message through and replace all others with errors" in forAll(
    nonEmptyVectorOfInts
  ) { xs: Vector[Int] =>
    val error = "Error"
    val errorNum = Math.max(xs.size - 1, 0)
    val expected: Vector[String \/ Int] =
      xs.take(1).map(\/-(_)) ++ Vector.fill(errorNum)(-\/(error))
    val input: Source[String \/ Int, NotUsed] =
      Source.fromIterator(() => xs.iterator).map(\/-(_))

    val actualF: Future[Vector[String \/ Int]] =
      input
        .via(allowOnlyFirstInput[String, Int](error))
        .runFold(Vector.empty[String \/ Int])(_ :+ _)

    whenReady(actualF, timeout(5.seconds), interval(100.milliseconds)) { actual =>
      actual shouldBe expected
    }
  }

  private val nonEmptyVectorOfInts: Gen[Vector[Int]] =
    Gen.nonEmptyBuildableOf[Vector[Int], Int](Arbitrary.arbitrary[Int])
}
