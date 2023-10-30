// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import java.time.Instant

import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import spray.json._

import scala.util.{Success, Try}

class AuthServiceJWTCodecSpec
    extends AnyWordSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  /** Serializes a [[AuthServiceJWTPayload]] to JSON, then parses it back to a AuthServiceJWTPayload */
  private def serializeAndParse(
      value: AuthServiceJWTPayload
  )(implicit format: RootJsonFormat[AuthServiceJWTPayload]): Try[AuthServiceJWTPayload] =
    for {
      serialized <- Try(value.toJson.prettyPrint)
      json <- Try(serialized.parseJson)
      parsed <- Try(json.convertTo[AuthServiceJWTPayload])
    } yield parsed

  /** Parses a [[AuthServiceJWTPayload]] */
  private def parse(
      serialized: String
  )(implicit format: RootJsonFormat[AuthServiceJWTPayload]): Try[AuthServiceJWTPayload] =
    for {
      json <- Try(serialized.parseJson)
      parsed <- Try(json.convertTo[AuthServiceJWTPayload])
    } yield parsed

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
        StandardJWTTokenFormat.Audience,
        StandardJWTTokenFormat.Scope,
      )
    )

  // participantId is mandatory for the format `StandardJWTTokenFormat.Audience`
  private val StandardJWTPayloadGen = {
    Gen
      .resultOf(StandardJWTPayload)
      .filterNot { payload =>
        payload.participantId
          .forall(_.isEmpty) && payload.format == StandardJWTTokenFormat.Audience
      }
      .filterNot { payload =>
        payload.scope.forall(_.isEmpty) && payload.format == StandardJWTTokenFormat.Scope

      }
      // we do not fill audiences for Scope or Audience based tokens
      .map(payload => payload.copy(audiences = List.empty))
      // we coerce all scopes to contain the official ledger api string, we test the non-conforming ones separately
      .map(payload =>
        payload.copy(scope = payload.scope.map(_ => AuthServiceJWTCodec.scopeLedgerApiFull))
      )
  }

  "Audience-Based AuthServiceJWTPayload codec" when {
    import AuthServiceJWTCodec.AudienceBasedTokenJsonImplicits._

    val PayloadGen = Gen
      .resultOf(StandardJWTPayload)
      .map(payload => payload.copy(participantId = None, format = StandardJWTTokenFormat.Audience))

    "serializing and parsing a value" should {
      "work for arbitrary custom Daml token values" in forAll(
        PayloadGen,
        minSuccessful(100),
      )(value => {
        serializeAndParse(value) shouldBe Success(value)
      })
    }

    "support multiple audiences with a single participant audience" in {
      val serialized =
        """{
          |  "aud": ["https://example.com/non/related/audience",
          |          "https://daml.com/jwt/aud/participant/someParticipantId"],
          |  "sub": "someUserId",
          |  "exp": 100
          |}
        """.stripMargin
      parse(serialized) shouldBe Success(
        StandardJWTPayload(
          issuer = None,
          participantId = None,
          userId = "someUserId",
          exp = Some(Instant.ofEpochSecond(100)),
          format = StandardJWTTokenFormat.Audience,
          audiences = List(
            "https://example.com/non/related/audience",
            "https://daml.com/jwt/aud/participant/someParticipantId",
          ),
          scope = None,
        )
      )
    }
  }

  "Scope-Based AuthServiceJWTPayload codec" when {
    import AuthServiceJWTCodec.ScopeBasedTokenJsonImplicits._

    val PayloadGen = Gen
      .resultOf(StandardJWTPayload)
      .map(payload => payload.copy(participantId = None, format = StandardJWTTokenFormat.Scope))

    "serializing and parsing a value" should {
      "work for arbitrary custom Daml token values" in forAll(
        PayloadGen,
        minSuccessful(100),
      )(value => {
        serializeAndParse(value) shouldBe Success(value)
      })
    }
  }

  "AuthServiceJWTPayload codec" when {
    import AuthServiceJWTCodec.JsonImplicits._

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
          audiences = List.empty,
          scope = Some(AuthServiceJWTCodec.scopeLedgerApiFull),
        )
        parse(serialized) shouldBe Success(expected)
      }

      "reject standard JWT claims with one composite scope" in {
        val serialized =
          """{
            |  "iss": "issuer",
            |  "aud": "someParticipantId",
            |  "sub": "someUserId",
            |  "exp": 100,
            |  "scope": "resource_server/daml_ledger_api"
            |}
          """.stripMargin
        parse(serialized).failed.get.getMessage should include(
          "Access token with unknown scope"
        )
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
          audiences = List.empty,
          scope = Some(AuthServiceJWTCodec.scopeLedgerApiFull),
        )
        parse(serialized) shouldBe Success(expected)
      }

      "support standard JWT claims with extra composite scopes" in {
        val serialized =
          """{
            |  "aud": "someParticipantId",
            |  "sub": "someUserId",
            |  "exp": 100,
            |  "scope": "resource_server/dummy-scope1 daml_ledger_api resource_server/dummy-scope2"
            |}
          """.stripMargin
        val expected = StandardJWTPayload(
          issuer = None,
          participantId = Some("someParticipantId"),
          userId = "someUserId",
          exp = Some(Instant.ofEpochSecond(100)),
          format = StandardJWTTokenFormat.Scope,
          audiences = List.empty,
          scope = Some(AuthServiceJWTCodec.scopeLedgerApiFull),
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
          audiences = List.empty,
          scope = Some(AuthServiceJWTCodec.scopeLedgerApiFull),
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
          audiences = List.empty,
          scope = Some(AuthServiceJWTCodec.scopeLedgerApiFull),
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
          format = StandardJWTTokenFormat.Audience,
          audiences = List.empty,
          scope = None,
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
            format = StandardJWTTokenFormat.Audience,
            audiences = List.empty,
            scope = None,
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
            audiences = List.empty,
            scope = Some(AuthServiceJWTCodec.scopeLedgerApiFull),
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
          format = StandardJWTTokenFormat.Audience,
          audiences = List.empty,
          scope = Some(AuthServiceJWTCodec.scopeLedgerApiFull),
        )
        parse(serialized) shouldBe Success(expected)
      }

      "support multiple audiences with a single participant audience" in {
        val serialized =
          """{
            |  "aud": ["https://example.com/non/related/audience",
            |          "https://daml.com/jwt/aud/participant/someParticipantId"],
            |  "sub": "someUserId",
            |  "exp": 100
            |}
          """.stripMargin
        parse(serialized) shouldBe Success(
          StandardJWTPayload(
            issuer = None,
            participantId = Some("someParticipantId"),
            userId = "someUserId",
            exp = Some(Instant.ofEpochSecond(100)),
            format = StandardJWTTokenFormat.Audience,
            audiences = List.empty,
            scope = None,
          )
        )
      }

      "reject the token of ParticipantId format with multiple participant audiences" in {
        val serialized =
          """{
            |  "aud": ["https://daml.com/jwt/aud/participant/someParticipantId",
            |          "https://daml.com/jwt/aud/participant/someParticipantId2"],
            |  "sub": "someUserId",
            |  "exp": 100
            |}
          """.stripMargin
        parse(serialized).failed.get.getMessage should include(
          "must include a single participantId value prefixed by"
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
        parse(serialized).failed.get.getMessage should include(
          "`aud` must be empty or a single participantId."
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

      "reject token with invalid scope" in {
        val serialized =
          """{
            |  "iss": "issuer",
            |  "aud": "someParticipantId",
            |  "sub": "someUserId",
            |  "exp": 100,
            |  "scope": "resource-server/daml-ledger-api"
            |}
          """.stripMargin
        parse(serialized).failed.get.getMessage should include(
          "Access token with unknown scope"
        )
      }
    }
  }
}
