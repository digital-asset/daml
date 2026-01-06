// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import com.digitalasset.canton.http.json.ResponseFormats
import io.circe.Json
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.{StatusCode, StatusCodes}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import org.scalacheck.Gen
import org.scalatest.Inside
import org.scalatest.compatible.Assertion
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import scalaz.syntax.show.*
import scalaz.{Show, \/}

import scala.concurrent.duration.*
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
    val jsValWarnings: Option[Json] = warnings.map(ws => Json.arr(ws.map(w => Json.fromString(w))*))
    val (failures, successes): (Vector[Json], Vector[Json]) =
      input.toVector.partitionMap(_.leftMap(e => Json.fromString(e.shows)).toEither)

    val (wantResponse, wantStatus) = expectedResult(failures, successes, jsValWarnings)

    val jsValSource = Source[DummyError \/ Json](input)

    val resultF: Future[Assertion] = ResponseFormats
      .resultJsObject(jsValSource, jsValWarnings)
      .flatMap { case (source, statusCode) =>
        source.runFold(ByteString.empty)(_ ++ _).map(bytes => (bytes, statusCode))
      }
      .map { case (bytes, statusCode) =>
        statusCode shouldBe wantStatus
        bytes.utf8String shouldBe wantResponse.noSpaces
      }

    Await.result(resultF, 10.seconds)
  }

  private def expectedResult(
      failures: Vector[Json],
      successes: Vector[Json],
      warnings: Option[Json],
  ): (Json, StatusCode) = {

    val map1: Map[String, Json] = warnings match {
      case Some(x) => Map("warnings" -> x)
      case None => Map.empty
    }

    val (map2, status) =
      if (failures.isEmpty)
        (
          Map[String, Json]("result" -> Json.fromValues(successes), "status" -> Json.fromInt(200)),
          StatusCodes.OK,
        )
      else
        (
          Map[String, Json]("errors" -> Json.fromValues(failures), "status" -> Json.fromInt(500)),
          StatusCodes.InternalServerError: StatusCode,
        )
    (Json.fromFields(map1 ++ map2), status)
  }

  private lazy val errorOrJsNumber: Gen[DummyError \/ Json] = Gen.frequency(
    1 -> dummyErrorGen.map(\/.left),
    5 -> jsNumberGen.map(\/.right),
  )

  private lazy val dummyErrorGen: Gen[DummyError] = Gen.identifier.map(DummyError.apply)

  private lazy val jsNumberGen: Gen[Json] = Gen.posNum[Long].map(Json.fromLong)
}

final case class DummyError(message: String)

object DummyError {
  implicit val ShowInstance: Show[DummyError] = Show shows { e =>
    s"DummyError(${e.message})"
  }
}
