// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.scalacheck.Gen
import org.scalatest.compatible.Assertion
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FreeSpec, Inside, Matchers}
import scalaz.syntax.show._
import scalaz.{-\/, Show, \/, \/-}
import spray.json.{JsArray, JsNumber, JsObject, JsString, JsValue, JsonParser}

import scala.collection.breakOut
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class ResponseFormatsTest
    extends FreeSpec
    with Matchers
    with Inside
    with GeneratorDrivenPropertyChecks {

  implicit val asys: ActorSystem = ActorSystem(this.getClass.getSimpleName)
  implicit val mat: Materializer = Materializer(asys)
  implicit val ec: ExecutionContext = asys.dispatcher

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  "resultJsObject should serialize Source of Errors and JsValues" in forAll(
    Gen.listOf(errorOrJsNumber)) { input =>
    val successes: Vector[JsValue] = input.collect { case \/-(jsValue) => jsValue }(breakOut)
    val failures: Vector[JsString] =
      input.collect { case -\/(e) => e }.map(e => JsString(e.shows))(breakOut)

    val expected: JsValue =
      if (failures.isEmpty)
        JsObject("result" -> JsArray(successes), "status" -> JsNumber("200"))
      else
        JsObject(
          "result" -> JsArray(successes),
          "errors" -> JsArray(failures),
          "status" -> JsNumber("501"))

    val source = Source[DummyError \/ JsValue](input)

    val responseF: Future[ByteString] =
      ResponseFormats.resultJsObject(source).runFold(ByteString.empty)((b, a) => b ++ a)

    val resultF: Future[Assertion] = responseF.map { str =>
      JsonParser(str.utf8String) shouldBe expected
    }

    Await.result(resultF, 10.seconds)
  }
  private lazy val errorOrJsNumber: Gen[DummyError \/ JsNumber] = Gen.frequency(
    1 -> dummyErrorGen.map(\/.left),
    5 -> jsNumberGen.map(\/.right)
  )

  private lazy val dummyErrorGen: Gen[DummyError] = Gen.identifier.map(DummyError.apply)

  private lazy val jsNumberGen: Gen[JsNumber] = Gen.posNum[Long].map(JsNumber.apply)
}

final case class DummyError(message: String)

object DummyError {
  implicit val ShowInstance: Show[DummyError] = Show shows { e =>
    s"DummyError(${e.message})"
  }
}
