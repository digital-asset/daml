// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import org.scalacheck.{Arbitrary, Gen}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import spray.json._

import java.time.Instant
import scala.util.{Failure, Success, Try}

class AuthServiceJWTCodecSpec
    extends AnyWordSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  /** Serializes a [[AuthServiceJWTPayload]] to JSON, then parses it back to a AuthServiceJWTPayload */
  private def serializeAndParse(value: AuthServiceJWTPayload): Try[AuthServiceJWTPayload] = {
    import AuthServiceJWTCodec.JsonImplicits._

    for {
      serialized <- Try(value.toJson.prettyPrint)
      json <- Try(serialized.parseJson)
      parsed <- Try(json.convertTo[AuthServiceJWTPayload])
    } yield parsed
  }

  /** Parses a [[AuthServiceJWTPayload]] */
  private def parse(serialized: String): Try[AuthServiceJWTPayload] = {
    import AuthServiceJWTCodec.JsonImplicits._

    for {
      json <- Try(serialized.parseJson)
      parsed <- Try(json.convertTo[AuthServiceJWTPayload])
    } yield parsed
  }

  private implicit val arbInstant: Arbitrary[Instant] = {
    Arbitrary {
      for {
        seconds <- Gen.chooseNum(Instant.MIN.getEpochSecond, Instant.MAX.getEpochSecond)
      } yield {
        Instant.ofEpochSecond(seconds)
      }
    }
  }

  private implicit val arbFormat: Arbitrary[StandardJWTTokenFormat] =
    Arbitrary(
      Gen.oneOf[StandardJWTTokenFormat](
        StandardJWTTokenFormat.ParticipantId,
        StandardJWTTokenFormat.Scope,
      )
    )

  // participantId is mandatory for the format `StandardJWTTokenFormat.ParticipantId`
  private val StandardJWTPayloadGen = Gen.resultOf(StandardJWTPayload).filterNot { payload =>
    !payload.participantId
      .exists(_.nonEmpty) && payload.format == StandardJWTTokenFormat.ParticipantId
  }

  "AuthServiceJWTPayload codec" when {

    "serializing and parsing a value" should {

      "work for arbitrary custom Daml token values" in forAll(
        Gen.resultOf(CustomDamlJWTPayload),
        minSuccessful(100),
      )(value => {
        serializeAndParse(value) shouldBe Success(value)
      })

      "work for arbitrary standard Daml token values" in forAll(
        StandardJWTPayloadGen,
        minSuccessful(100),
      ) { value =>
        serializeAndParse(value) shouldBe Success(value)
      }

      "support OIDC compliant sandbox format" in {
        val serialized =
          """{
            |  "https://daml.com/ledger-api": {
            |    "ledgerId": "someLedgerId",
            |    "participantId": "someParticipantId",
            |    "applicationId": "someApplicationId",
            |    "admin": true,
            |    "actAs": ["Alice"],
            |    "readAs": ["Alice", "Bob"]
            |  },
            |  "exp": 0
            |}
          """.stripMargin
        val expected = CustomDamlJWTPayload(
          ledgerId = Some("someLedgerId"),
          participantId = Some("someParticipantId"),
          applicationId = Some("someApplicationId"),
          exp = Some(Instant.EPOCH),
          admin = true,
          actAs = List("Alice"),
          readAs = List("Alice", "Bob"),
        )
        expected.party shouldBe None
        parse(serialized) shouldBe Success(expected)
      }

      "support legacy sandbox format" in {
        val serialized =
          """{
            |  "ledgerId": "someLedgerId",
            |  "participantId": "someParticipantId",
            |  "applicationId": "someApplicationId",
            |  "exp": 0,
            |  "admin": true,
            |  "actAs": ["Alice"],
            |  "readAs": ["Alice", "Bob"]
            |}
          """.stripMargin
        val expected = CustomDamlJWTPayload(
          ledgerId = Some("someLedgerId"),
          participantId = Some("someParticipantId"),
          applicationId = Some("someApplicationId"),
          exp = Some(Instant.EPOCH),
          admin = true,
          actAs = List("Alice"),
          readAs = List("Alice", "Bob"),
        )
        expected.party shouldBe None
        parse(serialized) shouldBe Success(expected)
      }

      "support legacy JSON API format" in {
        val serialized =
          """{
            |  "ledgerId": "someLedgerId",
            |  "applicationId": "someApplicationId",
            |  "party": "Alice"
            |}
          """.stripMargin
        val expected = CustomDamlJWTPayload(
          ledgerId = Some("someLedgerId"),
          participantId = None,
          applicationId = Some("someApplicationId"),
          exp = None,
          admin = false,
          actAs = List("Alice"),
          readAs = List.empty,
        )
        expected.party shouldBe Some("Alice")
        parse(serialized) shouldBe Success(expected)
      }

      "support legacy sandbox format even with standard JWT claims present" in {
        val serialized =
          """{
            |  "participantId": "someLegacyParticipantId",
            |  "aud": "someParticipantId",
            |  "sub": "someUserId",
            |  "exp": 100,
            |  "scope": "dummy-scope1 dummy-scope2"
            |}
          """.stripMargin
        val expected = CustomDamlJWTPayload(
          ledgerId = None,
          participantId = Some("someLegacyParticipantId"),
          applicationId = None,
          exp = Some(Instant.ofEpochSecond(100)),
          admin = false,
          actAs = List.empty,
          readAs = List.empty,
        )
        parse(serialized) shouldBe Success(expected)
      }

      "support standard JWT claims with just the one scope" in {
        val serialized =
          """{
            |  "iss": "issuer",
            |  "aud": "someParticipantId",
            |  "sub": "someUserId",
            |  "exp": 100,
            |  "scope": "daml_ledger_api"
            |}
          """.stripMargin
        val expected = StandardJWTPayload(
          issuer = Some("issuer"),
          participantId = Some("someParticipantId"),
          userId = "someUserId",
          exp = Some(Instant.ofEpochSecond(100)),
          format = StandardJWTTokenFormat.Scope,
        )
        parse(serialized) shouldBe Success(expected)
      }

      "support standard JWT claims with extra scopes" in {
        val serialized =
          """{
            |  "aud": "someParticipantId",
            |  "sub": "someUserId",
            |  "exp": 100,
            |  "scope": "dummy-scope1 daml_ledger_api dummy-scope2"
            |}
          """.stripMargin
        val expected = StandardJWTPayload(
          issuer = None,
          participantId = Some("someParticipantId"),
          userId = "someUserId",
          exp = Some(Instant.ofEpochSecond(100)),
          format = StandardJWTTokenFormat.Scope,
        )
        parse(serialized) shouldBe Success(expected)
      }

      "support standard JWT claims with iss claim as string" in {
        val serialized =
          """{
            |  "iss": "issuer1",
            |  "sub": "someUserId",
            |  "scope": "daml_ledger_api"
            |}
          """.stripMargin
        val expected = StandardJWTPayload(
          issuer = Some("issuer1"),
          participantId = None,
          userId = "someUserId",
          exp = None,
          format = StandardJWTTokenFormat.Scope,
        )
        parse(serialized) shouldBe Success(expected)
      }

      "support standard JWT claims with iss claim as URL" in {
        val serialized =
          """{
            |  "iss": "http://daml.com/",
            |  "sub": "someUserId",
            |  "scope": "daml_ledger_api"
            |}""".stripMargin
        val expected = StandardJWTPayload(
          issuer = Some("http://daml.com/"),
          participantId = None,
          userId = "someUserId",
          exp = None,
          format = StandardJWTTokenFormat.Scope,
        )
        parse(serialized) shouldBe Success(expected)
      }

      "have stable default values" in {
        val serialized = "{}"
        val expected = CustomDamlJWTPayload(
          ledgerId = None,
          participantId = None,
          applicationId = None,
          exp = None,
          admin = false,
          actAs = List.empty,
          readAs = List.empty,
        )
        expected.party shouldBe None
        parse(serialized) shouldBe Success(expected)
      }

      "support additional daml user token with prefixed audience" in {
        val serialized =
          """{
            |  "aud": "https://daml.com/jwt/aud/participant/someParticipantId",
            |  "sub": "someUserId",
            |  "exp": 100
            |}
          """.stripMargin
        val expected = StandardJWTPayload(
          issuer = None,
          participantId = Some("someParticipantId"),
          userId = "someUserId",
          exp = Some(Instant.ofEpochSecond(100)),
          format = StandardJWTTokenFormat.ParticipantId,
        )
        parse(serialized) shouldBe Success(expected)
      }

      "treat a singleton array of audiences equivalent to a string of its first element" in {
        val prefixed =
          """{
            |  "aud": ["https://daml.com/jwt/aud/participant/someParticipantId"],
            |  "sub": "someUserId",
            |  "exp": 100
            |}
          """.stripMargin
        parse(prefixed) shouldBe Success(
          StandardJWTPayload(
            issuer = None,
            participantId = Some("someParticipantId"),
            userId = "someUserId",
            exp = Some(Instant.ofEpochSecond(100)),
            format = StandardJWTTokenFormat.ParticipantId,
          )
        )

        val standard =
          """{
            |  "aud": ["someParticipantId"],
            |  "sub": "someUserId",
            |  "exp": 100,
            |  "scope": "dummy-scope1 daml_ledger_api dummy-scope2"
            |}
          """.stripMargin
        parse(standard) shouldBe Success(
          StandardJWTPayload(
            issuer = None,
            participantId = Some("someParticipantId"),
            userId = "someUserId",
            exp = Some(Instant.ofEpochSecond(100)),
            format = StandardJWTTokenFormat.Scope,
          )
        )
      }

      "support additional daml user token with prefixed audience and provided scope" in {
        val serialized =
          """{
            |  "aud": ["https://daml.com/jwt/aud/participant/someParticipantId"],
            |  "sub": "someUserId",
            |  "exp": 100,
            |  "scope": "daml_ledger_api"
            |}
          """.stripMargin
        val expected = StandardJWTPayload(
          issuer = None,
          participantId = Some("someParticipantId"),
          userId = "someUserId",
          exp = Some(Instant.ofEpochSecond(100)),
          format = StandardJWTTokenFormat.ParticipantId,
        )
        parse(serialized) shouldBe Success(expected)
      }

      "reject the token of ParticipantId format with multiple audiences" in {
        val serialized =
          """{
            |  "aud": ["https://daml.com/jwt/aud/participant/someParticipantId", 
            |          "https://daml.com/jwt/aud/participant/someParticipantId2"],
            |  "sub": "someUserId",
            |  "exp": 100
            |}
          """.stripMargin
        parse(serialized) shouldBe Failure(
          DeserializationException(
            "Could not read [\"https://daml.com/jwt/aud/participant/someParticipantId\", " +
              "\"https://daml.com/jwt/aud/participant/someParticipantId2\"] as string for aud"
          )
        )
      }

      "reject the token of Scope format with multiple audiences" in {
        val serialized =
          """{
            |  "aud": ["someParticipantId", 
            |          "someParticipantId2"],
            |  "sub": "someUserId",
            |  "exp": 100,
            |  "scope": "daml_ledger_api"
            |}
          """.stripMargin
        parse(serialized) shouldBe Failure(
          DeserializationException(
            "Could not read [\"someParticipantId\", " +
              "\"someParticipantId2\"] as string for aud"
          )
        )
      }

      "reject the ParticipantId format token with empty participantId" in {
        val serialized =
          """{
            |  "aud": ["https://daml.com/jwt/aud/participant/"],
            |  "sub": "someUserId",
            |  "exp": 100
            |}
          """.stripMargin
        parse(serialized).failed.get.getMessage
          .contains("must include participantId value prefixed by") shouldBe true
      }
    }
  }
}
