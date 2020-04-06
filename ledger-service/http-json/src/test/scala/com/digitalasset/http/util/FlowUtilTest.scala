// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import scalaz.{-\/, \/, \/-}

import scala.concurrent.Future

class FlowUtilTest
    extends FlatSpec
    with ScalaFutures
    with Matchers
    with GeneratorDrivenPropertyChecks {
  import FlowUtil._

  implicit val asys: ActorSystem = ActorSystem(this.getClass.getSimpleName)
  implicit val materializer: Materializer = Materializer(asys)

  "allowOnlyFirstInput" should "pass 1st message through and replace all others with errors" in forAll {
    xs: Vector[Int] =>
      val error = "Error"
      val errorNum = Math.max(xs.size - 1, 0)
      val expected: Vector[String \/ Int] =
        xs.take(1).map(\/-(_)) ++ Vector.fill(errorNum)(-\/(error))
      val input: Source[String \/ Int, NotUsed] =
        Source.fromIterator(() => xs.toIterator).map(\/-(_))

      val actualF: Future[Vector[String \/ Int]] =
        input
          .via(allowOnlyFirstInput[String, Int](error))
          .runFold(Vector.empty[String \/ Int])(_ :+ _)

      whenReady(actualF) { actual =>
        actual shouldBe expected
      }
  }
}
