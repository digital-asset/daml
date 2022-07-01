// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import org.scalacheck.{Arbitrary, Gen}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import spray.json._

import java.time.Instant
import scala.util.{Success, Try}

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

  "AuthServiceJWTPayload codec" when {

    "serializing and parsing a value" should {

      "work for arbitrary custom Daml token values" in forAll(
        Gen.resultOf(CustomDamlJWTPayload),
        minSuccessful(100),
      )(value => {
        serializeAndParse(value) shouldBe Success(value)
      })

      "work for arbitrary standard Daml token values" in forAll(
        Gen.resultOf(StandardJWTPayload),
        minSuccessful(100),
      ) { value =>
        // `participantId` is being used to identify the format of the token `ParticipantId` or `Scope`.
        // It is impossible to distinguish empty string `participantId` and non-provided `participantId`
        val filtered = value.copy(participantId = value.participantId.filter(_.nonEmpty))
        serializeAndParse(filtered) shouldBe Success(filtered)
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
            |  "aud": "someParticipantId",
            |  "sub": "someUserId",
            |  "exp": 100,
            |  "scope": "daml_ledger_api"
            |}
          """.stripMargin
        val expected = StandardJWTPayload(
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
          participantId = Some("someParticipantId"),
          userId = "someUserId",
          exp = Some(Instant.ofEpochSecond(100)),
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
            participantId = Some("someParticipantId"),
            userId = "someUserId",
            exp = Some(Instant.ofEpochSecond(100)),
            format = StandardJWTTokenFormat.Scope,
          )
        )
      }
    }
  }
}
