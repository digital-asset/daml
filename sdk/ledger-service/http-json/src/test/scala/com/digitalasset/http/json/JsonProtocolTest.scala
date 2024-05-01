// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.json

import org.apache.pekko.http.scaladsl.model.StatusCodes
import com.daml.http.Generators.{
  OptionalPackageIdGen,
  contractGen,
  contractIdGen,
  contractLocatorGen,
  exerciseCmdGen,
  genDomainTemplateId,
  genDomainTemplateIdO,
  genServiceWarning,
  genUnknownParties,
  genUnknownTemplateIds,
  genWarningsWrapper,
}
import com.daml.http.domain
import com.daml.http.json.SprayJson.JsonReaderError
import com.daml.lf.data.Ref
import com.daml.scalautil.Statement.discard
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Arbitrary
import org.scalacheck.Gen.{identifier, listOf}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, Succeeded}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import scalaz.syntax.functor._
import scalaz.syntax.std.option._
import scalaz.syntax.tag._
import scalaz.{\/, \/-, -\/}

class JsonProtocolTest
    extends AnyFreeSpec
    with Matchers
    with Inside
    with ScalaCheckDrivenPropertyChecks {

  import JsonProtocol._
  import JsonProtocolTest._
  import spray.json._

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  "domain.ContractTypeId.RequiredPkg" - {
    "can be serialized to JSON" in forAll(genDomainTemplateId) {
      a: domain.ContractTypeId.RequiredPkg =>
        inside(a.toJson) { case JsString(str) =>
          str should ===(s"${a.packageId}:${a.moduleName}:${a.entityName}")
        }
    }
    "roundtrips" in forAll(genDomainTemplateId) { a: domain.ContractTypeId.RequiredPkg =>
      val b = a.toJson.convertTo[domain.ContractTypeId.RequiredPkg]
      b should ===(a)
    }
  }

  "domain.ContractTypeId.OptionalPkg" - {
    "can be serialized to JSON" in forAll(genDomainTemplateIdO) {
      a: domain.ContractTypeId.OptionalPkg =>
        val expectedStr: String = a.packageId.cata(
          p => s"${p: String}:${a.moduleName}:${a.entityName}",
          s"${a.moduleName}:${a.entityName}",
        )

        inside(a.toJson) { case JsString(str) =>
          str should ===(expectedStr)
        }
    }
    "roundtrips" in forAll(genDomainTemplateIdO) { a: domain.ContractTypeId.OptionalPkg =>
      val b = a.toJson.convertTo[domain.ContractTypeId.OptionalPkg]
      b should ===(a)
    }
  }

  "domain.Base16" - {
    "is case-insensitive" in forAll { b16: domain.Base16 =>
      val str = b16.toJson.convertTo[String]
      all(
        Seq(str.toUpperCase, str.toLowerCase)
          .map(_.toJson.convertTo[domain.Base16])
      ) should ===(b16)
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

  "domain.DeduplicationPeriod" - {
    @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
    def roundtrip(p: domain.DeduplicationPeriod, expected: JsValue) = {
      SprayJson.encode(p) should ===(\/-(expected))
      SprayJson.decode[domain.DeduplicationPeriod](expected) should ===(\/-(p))
    }

    "encodes durations" in {
      roundtrip(
        domain.DeduplicationPeriod.Duration(10000L),
        Map("type" -> "Duration".toJson, "durationInMillis" -> 10000L.toJson).toJson,
      )
    }

    "encodes offsets" in {
      roundtrip(
        domain.DeduplicationPeriod.Offset(Ref.HexString assertFromString "0123579236ab"),
        Map("type" -> "Offset", "offset" -> "0123579236ab").toJson,
      )
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
    "response with warnings" in forAll(listOf(genDomainTemplateIdO)) {
      templateIds: List[domain.ContractTypeId.OptionalPkg] =>
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
            List(domain.ContractTypeId(Option.empty[String], "AAA", "BBB"))
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
        ) ++ Iterable(
          cmd.choiceInterfaceId.map(x => "choiceInterfaceId" -> x.toJson),
          cmd.meta.map(x => "meta" -> x.toJson),
        ).collect { case Some(x) => x }

        actual shouldBe JsObject(expectedFields)
    }

    "roundtrips" in forAll(exerciseCmdGen) { a =>
      val b = a.toJson
        .convertTo[domain.ExerciseCommand.RequiredPkg[JsValue, domain.ContractLocator[JsValue]]]
      b should ===(a)
    }
  }

  "domain.CommandMeta" - {
    "is entirely optional" in {
      "{}".parseJson.convertTo[domain.CommandMeta[JsValue]] should ===(
        domain.CommandMeta(None, None, None, None, None, None)
      )
    }

    "is entirely optional when NoDisclosed" in {
      "{}".parseJson.convertTo[domain.CommandMeta.NoDisclosed] should ===(
        domain.CommandMeta(None, None, None, None, None, None)
      )
    }
  }

  "domain.DisclosedContract" - {
    import domain.DisclosedContract
    type DC = DisclosedContract[Int]

    "roundtrips" in forAll { a: DC =>
      val b = a.toJson.convertTo[DC]
      b should ===(a)
    }

    "decodes a hand-written sample" in {
      import com.google.protobuf.ByteString
      val utf8 = java.nio.charset.Charset forName "UTF-8"
      val expected = DisclosedContract(
        contractId = domain.ContractId("abcd"),
        templateId = domain.ContractTypeId.Template(Option.empty[String], "Mod", "Tmpl"),
        createdEventBlob = domain.Base64(ByteString.copyFrom("some create event payload", utf8)),
      )
      val encoded =
        s"""{
          "contractId": "abcd",
          "templateId": "Mod:Tmpl",
          "createdEventBlob": "c29tZSBjcmVhdGUgZXZlbnQgcGF5bG9hZA=="
          }""".parseJson
      val _ = expected.toJson should ===(encoded)
      val decoded =
        encoded.convertTo[DisclosedContract[domain.ContractTypeId.Template.OptionalPkg]]
      decoded should ===(expected)
    }

    "fails to decode with an empty createdEventBlob" in {
      val encoded =
        s"""{
        "contractId": "abcd",
        "templateId": "Mod:Tmpl",
        "createdEventBlob": ""
      }""".parseJson

      val result =
        SprayJson.decode[DisclosedContract[domain.ContractTypeId.Template.OptionalPkg]](encoded)
      inside(result) { case -\/(JsonReaderError(_, message)) =>
        message shouldBe "spray.json.DeserializationException: DisclosedContract.createdEventBlob must not be empty"
      }
    }
  }
}

object JsonProtocolTest {
  // like Arbitrary(arbitrary[T].map(f)) but with inferred `T`
  private[this] def arbArg[T: Arbitrary, R](
      f: T => R,
      filterExpr: R => Boolean = (_: R) => true,
  ): Arbitrary[R] =
    Arbitrary(arbitrary[T] map f filter filterExpr)

  private[this] implicit val arbBase64: Arbitrary[domain.Base64] =
    domain.Base64 subst arbArg(com.google.protobuf.ByteString.copyFrom(_: Array[Byte]))

  private implicit val arbBase16: Arbitrary[domain.Base16] =
    domain.Base16 subst arbArg(com.google.protobuf.ByteString.copyFrom(_: Array[Byte]))

  private[this] implicit val arbCid: Arbitrary[domain.ContractId] =
    Arbitrary(contractIdGen)

  private[http] implicit def arbDisclosedCt[TpId: Arbitrary]
      : Arbitrary[domain.DisclosedContract[TpId]] =
    arbArg((domain.DisclosedContract.apply[TpId] _).tupled, !_.createdEventBlob.unwrap.isEmpty)
}
