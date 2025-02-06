// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.http
import com.digitalasset.canton.http.Generators.{
  contractGen,
  contractIdGen,
  contractLocatorGen,
  exerciseCmdGen,
  genHttpTemplateId,
  genServiceWarning,
  genUnknownParties,
  genUnknownTemplateIds,
  genWarningsWrapper,
}
import com.digitalasset.canton.http.json.SprayJson.JsonReaderError
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.daml.lf.data.Ref
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.{identifier, listOf}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, Inside, Succeeded}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import scalaz.syntax.functor.*
import scalaz.syntax.tag.*
import scalaz.{-\/, \/, \/-}

class JsonProtocolTest
    extends AnyFreeSpec
    with Matchers
    with Inside
    with ScalaCheckDrivenPropertyChecks {

  import JsonProtocol.*
  import JsonProtocolTest.*
  import spray.json.*

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  "http.ContractTypeId.RequiredPkg" - {
    "can be serialized to JSON" in forAll(genHttpTemplateId) {
      (a: http.ContractTypeId.RequiredPkg) =>
        inside(a.toJson) { case JsString(str) =>
          str should ===(s"${a.packageId}:${a.moduleName}:${a.entityName}")
        }
    }
    "roundtrips" in forAll(genHttpTemplateId) { (a: http.ContractTypeId.RequiredPkg) =>
      val b = a.toJson.convertTo[http.ContractTypeId.RequiredPkg]
      b should ===(a)
    }
  }

  "http.Base16" - {
    "is case-insensitive" in forAll { (b16: http.Base16) =>
      val str = b16.toJson.convertTo[String]
      all(
        Seq(str.toUpperCase, str.toLowerCase)
          .map(_.toJson.convertTo[http.Base16])
      ) should ===(b16)
    }
  }

  "http.Contract" - {
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
        val actual: SprayJson.Error \/ http.Contract[JsValue] = for {
          jsValue <- SprayJson.encode(contract0)
          contract <- SprayJson.decode[http.Contract[JsValue]](jsValue)
        } yield contract

        inside(actual) { case \/-(contract1) =>
          contract1 shouldBe contract0
        }
    }
  }

  "http.ContractLocator" - {
    type Loc = http.ContractLocator[JsValue]
    "roundtrips" in forAll(contractLocatorGen(arbitrary[Int] map (JsNumber(_)))) { (locator: Loc) =>
      locator.toJson.convertTo[Loc] should ===(locator)
    }
  }

  "http.DeduplicationPeriod" - {
    @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
    def roundtrip(p: http.DeduplicationPeriod, expected: JsValue) = {
      SprayJson.encode(p) should ===(\/-(expected))
      SprayJson.decode[http.DeduplicationPeriod](expected) should ===(\/-(p))
    }

    "encodes durations" in {
      roundtrip(
        http.DeduplicationPeriod.Duration(10000L),
        Map("type" -> "Duration".toJson, "durationInMillis" -> 10000L.toJson).toJson,
      )
    }

    "encodes offsets" in {
      roundtrip(
        http.DeduplicationPeriod.Offset(Ref.HexString assertFromString "0123579236ab"),
        Map("type" -> "Offset", "offset" -> "0123579236ab").toJson,
      )
    }
  }

  "http.ServiceWarning" - {
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
      x.toJson.convertTo[http.ServiceWarning] === x
    }
  }

  "http.WarningsWrapper" - {
    "serialization" in forAll(genWarningsWrapper) { x =>
      inside(x.toJson) {
        case JsObject(fields) if fields.contains("warnings") && fields.size == 1 =>
          Succeeded
      }
    }
    "roundtrips" in forAll(genWarningsWrapper) { x =>
      x.toJson.convertTo[http.AsyncWarningsWrapper] === x
    }
  }

  "http.OkResponse" - {

    "response with warnings" in forAll(listOf(genHttpTemplateId)) {
      (templateIds: List[http.ContractTypeId.RequiredPkg]) =>
        val response: http.OkResponse[Int] =
          http.OkResponse(result = 100, warnings = Some(http.UnknownTemplateIds(templateIds)))

        val responseJsVal: http.OkResponse[JsValue] = response.map(_.toJson)

        discard {
          responseJsVal.toJson shouldBe JsObject(
            "result" -> JsNumber(100),
            "warnings" -> JsObject("unknownTemplateIds" -> templateIds.toJson),
            "status" -> JsNumber(200),
          )
        }
    }

    "response without warnings" in forAll(identifier) { str =>
      val response: http.OkResponse[String] =
        http.OkResponse(result = str, warnings = None)

      val responseJsVal: http.OkResponse[JsValue] = response.map(_.toJson)

      discard {
        responseJsVal.toJson shouldBe JsObject(
          "result" -> JsString(str),
          "status" -> JsNumber(200),
        )
      }
    }
  }

  "http.SyncResponse" - {
    "Ok response parsed" in {
      import SprayJson.decode1

      val str =
        """{"warnings":{"unknownTemplateIds":["ZZZ:AAA:BBB"]},"result":[],"status":200}"""

      inside(decode1[http.SyncResponse, List[JsValue]](str)) {
        case \/-(http.OkResponse(List(), Some(warning), StatusCodes.OK)) =>
          warning shouldBe http.UnknownTemplateIds(
            List(http.ContractTypeId(Ref.PackageRef.assertFromString("ZZZ"), "AAA", "BBB"))
          )
      }
    }
  }

  "ErrorDetail" - {
    "Encoding and decoding ResourceInfoDetail should result in the same object" in {
      val resourceInfoDetail: http.ErrorDetail = http.ResourceInfoDetail("test", "test")
      resourceInfoDetail shouldBe resourceInfoDetail.toJson.convertTo[http.ErrorDetail]
    }

    "Encoding and decoding RetryInfoDetail should result in the same object" in {
      val retryInfoDetail: http.ErrorDetail =
        http.RetryInfoDetail(
          http.RetryInfoDetailDuration(
            scala.concurrent.duration.Duration.Zero: scala.concurrent.duration.Duration
          )
        )
      retryInfoDetail shouldBe retryInfoDetail.toJson.convertTo[http.ErrorDetail]
    }

    "Encoding and decoding RequestInfoDetail should result in the same object" in {
      val requestInfoDetail: http.ErrorDetail = http.RequestInfoDetail("test")
      requestInfoDetail shouldBe requestInfoDetail.toJson.convertTo[http.ErrorDetail]
    }

    "Encoding and decoding ErrorInfoDetail should result in the same object" in {
      val errorInfoDetail: http.ErrorDetail =
        http.ErrorInfoDetail("test", Map("test" -> "test1", "test2" -> "test3"))
      errorInfoDetail shouldBe errorInfoDetail.toJson.convertTo[http.ErrorDetail]
    }
  }

  "UserRight" - {
    def testIsomorphic[T <: http.UserRight](original: T): Assertion =
      (original: http.UserRight).toJson.convertTo[http.UserRight] shouldBe original

    "Encoding and decoding ParticipantAdmin should result in the same object" in {
      testIsomorphic(http.ParticipantAdmin)
    }
    "Encoding and decoding IdentityProviderAdmin should result in the same object" in {
      testIsomorphic(http.IdentityProviderAdmin)
    }
    "Encoding and decoding CanActAs should result in the same object" in {
      testIsomorphic(http.CanActAs(http.Party("canActAs")))
    }
    "Encoding and decoding CanReadAs should result in the same object" in {
      testIsomorphic(http.CanReadAs(http.Party("canReadAs")))
    }
    "Encoding and decoding CanReadAsAnyParty should result in the same object" in {
      testIsomorphic(http.CanReadAsAnyParty)
    }
  }

  "http.ExerciseCommand" - {
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
        .convertTo[http.ExerciseCommand.RequiredPkg[JsValue, http.ContractLocator[JsValue]]]
      b should ===(a)
    }
  }

  "http.CommandMeta" - {
    "is entirely optional" in {
      "{}".parseJson.convertTo[http.CommandMeta[JsValue]] should ===(
        http.CommandMeta(None, None, None, None, None, None, None, None, None)
      )
    }

    "is entirely optional when NoDisclosed" in {
      "{}".parseJson.convertTo[http.CommandMeta.NoDisclosed] should ===(
        http.CommandMeta(None, None, None, None, None, None, None, None, None)
      )
    }

    "successfully parsed with synchronizerId" in {
      """{"synchronizerId":"x::synchronizer"}""".parseJson
        .convertTo[http.CommandMeta[JsValue]] should ===(
        http.CommandMeta(
          None,
          None,
          None,
          None,
          None,
          None,
          None,
          Some(SynchronizerId.tryFromString("x::synchronizer")),
          None,
        )
      )
    }
  }

  "http.DisclosedContract" - {
    import http.DisclosedContract
    type DC = DisclosedContract[Int]

    "roundtrips" in forAll { (a: DC) =>
      val b = a.toJson.convertTo[DC]
      b should ===(a)
    }

    "decodes a hand-written sample" in {
      import com.google.protobuf.ByteString
      val utf8 = java.nio.charset.Charset forName "UTF-8"
      val expected = DisclosedContract(
        contractId = http.ContractId("abcd"),
        templateId =
          http.ContractTypeId.Template(Ref.PackageRef.assertFromString("Pkg"), "Mod", "Tmpl"),
        createdEventBlob = http.Base64(ByteString.copyFrom("some create event payload", utf8)),
      )
      val encoded =
        s"""{
        "contractId": "abcd",
        "templateId": "Pkg:Mod:Tmpl",
        "createdEventBlob": "c29tZSBjcmVhdGUgZXZlbnQgcGF5bG9hZA=="
      }""".parseJson
      val _ = expected.toJson should ===(encoded)
      val decoded =
        encoded.convertTo[DisclosedContract[http.ContractTypeId.Template.RequiredPkg]]
      decoded should ===(expected)
    }

    "fails to decode with an empty createdEventBlob" in {
      val encoded =
        s"""{
        "contractId": "abcd",
        "templateId": "Pkg:Mod:Tmpl",
        "createdEventBlob": ""
      }""".parseJson

      val result =
        SprayJson.decode[DisclosedContract[http.ContractTypeId.Template.RequiredPkg]](encoded)
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

  private[this] implicit val arbBase64: Arbitrary[http.Base64] =
    http.Base64 subst arbArg(com.google.protobuf.ByteString.copyFrom(_: Array[Byte]))

  private implicit val arbBase16: Arbitrary[http.Base16] =
    http.Base16 subst arbArg(com.google.protobuf.ByteString.copyFrom(_: Array[Byte]))

  private[this] implicit val arbCid: Arbitrary[http.ContractId] =
    Arbitrary(contractIdGen)

  private[http] implicit def arbDisclosedCt[TpId: Arbitrary]
      : Arbitrary[http.DisclosedContract[TpId]] =
    arbArg((http.DisclosedContract.apply[TpId] _).tupled, !_.createdEventBlob.unwrap.isEmpty)
}
