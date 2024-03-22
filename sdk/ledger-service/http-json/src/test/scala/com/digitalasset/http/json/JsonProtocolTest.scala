// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.json

import akka.http.scaladsl.model.StatusCodes
import com.daml.http.Generators.{
  OptionalPackageIdGen,
  contractGen,
  contractLocatorGen,
  exerciseCmdGen,
  genDomainTemplateId,
  genDomainTemplateIdO,
  genServiceWarning,
  genUnknownParties,
  genUnknownTemplateIds,
  genWarningsWrapper,
}
import com.daml.scalautil.Statement.discard
import com.daml.http.domain
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.{identifier, listOf}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.{Inside, Succeeded}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import scalaz.syntax.functor._
import scalaz.syntax.std.option._
import scalaz.syntax.tag._
import scalaz.{\/, \/-}

class JsonProtocolTest
    extends AnyFreeSpec
    with Matchers
    with Inside
    with ScalaCheckDrivenPropertyChecks {

  import JsonProtocol._
  import spray.json._

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  "domain.TemplateId.RequiredPkg" - {
    "can be serialized to JSON" in forAll(genDomainTemplateId) { a: domain.TemplateId.RequiredPkg =>
      inside(a.toJson) { case JsString(str) =>
        str should ===(s"${a.packageId}:${a.moduleName}:${a.entityName}")
      }
    }
    "roundtrips" in forAll(genDomainTemplateId) { a: domain.TemplateId.RequiredPkg =>
      val b = a.toJson.convertTo[domain.TemplateId.RequiredPkg]
      b should ===(a)
    }
  }

  "domain.TemplateId.OptionalPkg" - {
    "can be serialized to JSON" in forAll(genDomainTemplateIdO(OptionalPackageIdGen)) {
      a: domain.TemplateId.OptionalPkg =>
        val expectedStr: String = a.packageId.cata(
          p => s"${p: String}:${a.moduleName}:${a.entityName}",
          s"${a.moduleName}:${a.entityName}",
        )

        inside(a.toJson) { case JsString(str) =>
          str should ===(expectedStr)
        }
    }
    "roundtrips" in forAll(genDomainTemplateIdO) { a: domain.TemplateId.OptionalPkg =>
      val b = a.toJson.convertTo[domain.TemplateId.OptionalPkg]
      b should ===(a)
    }
  }

  "domain.Contract" - {
    "can be serialized to JSON" in forAll(contractGen) { contract =>
      inside(SprayJson.encode(contract)) { case \/-(JsObject(fields)) =>
        inside(fields.toList) {
          case List(("archived", JsObject(_))) =>
          case List(("created", JsObject(_))) =>
        }
      }
    }
    "can be serialized and deserialized back to the same object" in forAll(contractGen) {
      contract0 =>
        val actual: SprayJson.Error \/ domain.Contract[JsValue] = for {
          jsValue <- SprayJson.encode(contract0)
          contract <- SprayJson.decode[domain.Contract[JsValue]](jsValue)
        } yield contract

        inside(actual) { case \/-(contract1) =>
          contract1 shouldBe contract0
        }
    }
  }

  "domain.ContractLocator" - {
    type Loc = domain.ContractLocator[JsValue]
    "roundtrips" in forAll(contractLocatorGen(arbitrary[Int] map (JsNumber(_)))) { locator: Loc =>
      locator.toJson.convertTo[Loc] should ===(locator)
    }
  }

  "domain.ServiceWarning" - {
    "UnknownTemplateIds serialization" in forAll(genUnknownTemplateIds) { x =>
      val expectedTemplateIds: Vector[JsValue] = x.unknownTemplateIds.view.map(_.toJson).toVector
      val expected = JsObject("unknownTemplateIds" -> JsArray(expectedTemplateIds))
      x.toJson.asJsObject shouldBe expected
    }
    "UnknownParties serialization" in forAll(genUnknownParties) { x =>
      val expectedParties: Vector[JsValue] = x.unknownParties.view.map(_.toJson).toVector
      val expected = JsObject("unknownParties" -> JsArray(expectedParties))
      x.toJson.asJsObject shouldBe expected
    }
    "roundtrips" in forAll(genServiceWarning) { x =>
      x.toJson.convertTo[domain.ServiceWarning] === x
    }
  }

  "domain.WarningsWrapper" - {
    "serialization" in forAll(genWarningsWrapper) { x =>
      inside(x.toJson) {
        case JsObject(fields) if fields.contains("warnings") && fields.size == 1 =>
          Succeeded
      }
    }
    "roundtrips" in forAll(genWarningsWrapper) { x =>
      x.toJson.convertTo[domain.AsyncWarningsWrapper] === x
    }
  }

  "domain.OkResponse" - {

    "response with warnings" in forAll(listOf(genDomainTemplateIdO(OptionalPackageIdGen))) {
      templateIds: List[domain.TemplateId.OptionalPkg] =>
        val response: domain.OkResponse[Int] =
          domain.OkResponse(result = 100, warnings = Some(domain.UnknownTemplateIds(templateIds)))

        val responseJsVal: domain.OkResponse[JsValue] = response.map(_.toJson)

        discard {
          responseJsVal.toJson shouldBe JsObject(
            "result" -> JsNumber(100),
            "warnings" -> JsObject("unknownTemplateIds" -> templateIds.toJson),
            "status" -> JsNumber(200),
          )
        }
    }

    "response without warnings" in forAll(identifier) { str =>
      val response: domain.OkResponse[String] =
        domain.OkResponse(result = str, warnings = None)

      val responseJsVal: domain.OkResponse[JsValue] = response.map(_.toJson)

      discard {
        responseJsVal.toJson shouldBe JsObject(
          "result" -> JsString(str),
          "status" -> JsNumber(200),
        )
      }
    }
  }

  "domain.SyncResponse" - {
    "Ok response parsed" in {
      import SprayJson.decode1

      val str =
        """{"warnings":{"unknownTemplateIds":["AAA:BBB"]},"result":[],"status":200}"""

      inside(decode1[domain.SyncResponse, List[JsValue]](str)) {
        case \/-(domain.OkResponse(List(), Some(warning), StatusCodes.OK)) =>
          warning shouldBe domain.UnknownTemplateIds(
            List(domain.TemplateId(Option.empty[String], "AAA", "BBB"))
          )
      }
    }
  }

  "ErrorDetail" - {
    "Encoding and decoding ResourceInfoDetail should result in the same object" in {
      val resourceInfoDetail: domain.ErrorDetail = domain.ResourceInfoDetail("test", "test")
      resourceInfoDetail shouldBe resourceInfoDetail.toJson.convertTo[domain.ErrorDetail]
    }

    "Encoding and decoding RetryInfoDetail should result in the same object" in {
      val retryInfoDetail: domain.ErrorDetail =
        domain.RetryInfoDetail(
          domain.RetryInfoDetailDuration(
            scala.concurrent.duration.Duration.Zero: scala.concurrent.duration.Duration
          )
        )
      retryInfoDetail shouldBe retryInfoDetail.toJson.convertTo[domain.ErrorDetail]
    }

    "Encoding and decoding RequestInfoDetail should result in the same object" in {
      val requestInfoDetail: domain.ErrorDetail = domain.RequestInfoDetail("test")
      requestInfoDetail shouldBe requestInfoDetail.toJson.convertTo[domain.ErrorDetail]
    }

    "Encoding and decoding ErrorInfoDetail should result in the same object" in {
      val errorInfoDetail: domain.ErrorDetail =
        domain.ErrorInfoDetail("test", Map("test" -> "test1", "test2" -> "test3"))
      errorInfoDetail shouldBe errorInfoDetail.toJson.convertTo[domain.ErrorDetail]
    }
  }

  "domain.ExerciseCommand" - {
    "should serialize to a JSON object with flattened reference fields" in forAll(exerciseCmdGen) {
      cmd =>
        val actual: JsValue = cmd.toJson
        val referenceFields: Map[String, JsValue] = cmd.reference.toJson.asJsObject.fields
        val expectedFields: Map[String, JsValue] = referenceFields ++ Map[String, JsValue](
          "choice" -> JsString(cmd.choice.unwrap),
          "argument" -> cmd.argument,
        ) ++ cmd.meta.cata(x => Map("meta" -> x.toJson), Map.empty)

        actual shouldBe JsObject(expectedFields)
    }

    "roundtrips" in forAll(exerciseCmdGen) { a =>
      val b = a.toJson.convertTo[domain.ExerciseCommand[JsValue, domain.ContractLocator[JsValue]]]
      b should ===(a)
    }
  }
}
