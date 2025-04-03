// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.json

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.scalacheck.Gen
import org.scalatest.compatible.Assertion
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import scalaz.syntax.show._
import scalaz.{Show, \/}
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class ResponseFormatsTest
    extends AnyFreeSpec
    with Matchers
    with Inside
    with ScalaCheckDrivenPropertyChecks {

  implicit val asys: ActorSystem = ActorSystem(this.getClass.getSimpleName)
  implicit val mat: Materializer = Materializer(asys)
  implicit val ec: ExecutionContext = asys.dispatcher

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  "resultJsObject should serialize Source of Errors and JsValues" in forAll(
    Gen.listOf(errorOrJsNumber),
    Gen.option(Gen.nonEmptyListOf(Gen.identifier)),
  ) { (input, warnings) =>
    import spray.json.DefaultJsonProtocol._

    val jsValWarnings: Option[JsValue] = warnings.map(_.toJson)
    val (failures, successes): (Vector[JsString], Vector[JsValue]) =
      input.toVector.partitionMap(_.leftMap(e => JsString(e.shows)).toEither)

    val (wantResponse, wantStatus) = expectedResult(failures, successes, jsValWarnings)

    val jsValSource = Source[DummyError \/ JsValue](input)

    @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
    val resultF: Future[Assertion] = ResponseFormats
      .resultJsObject(jsValSource, jsValWarnings)
      .flatMap { case (source, statusCode) =>
        source.runFold(ByteString.empty)((b, a) => b ++ a).map(bytes => (bytes, statusCode))
      }
      .map { case (bytes, statusCode) =>
        statusCode shouldBe wantStatus
        JsonParser(bytes.utf8String) shouldBe wantResponse
      }

    Await.result(resultF, 10.seconds)
  }

  private def expectedResult(
      failures: Vector[JsValue],
      successes: Vector[JsValue],
      warnings: Option[JsValue],
  ): (JsObject, StatusCode) = {

    val map1: Map[String, JsValue] = warnings match {
      case Some(x) => Map("warnings" -> x)
      case None => Map.empty
    }

    val (map2, status) =
      if (failures.isEmpty)
        (
          Map[String, JsValue]("result" -> JsArray(successes), "status" -> JsNumber("200")),
          StatusCodes.OK,
        )
      else
        (
          Map[String, JsValue]("errors" -> JsArray(failures), "status" -> JsNumber("500")),
          StatusCodes.InternalServerError: StatusCode,
        )

    (JsObject(map1 ++ map2), status)
  }

  private lazy val errorOrJsNumber: Gen[DummyError \/ JsValue] = Gen.frequency(
    1 -> dummyErrorGen.map(\/.left),
    5 -> jsNumberGen.map(\/.right),
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
