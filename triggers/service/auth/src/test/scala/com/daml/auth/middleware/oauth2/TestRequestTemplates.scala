// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.oauth2

import java.io._
import java.nio.file.Path
import java.util.UUID
import org.apache.pekko.http.scaladsl.model.Uri
import com.daml.auth.middleware.api.Request.Claims
import com.daml.auth.middleware.api.Tagged.RefreshToken
import com.daml.lf.data.Ref
import com.daml.scalautil.Statement.discard
import org.scalatest.{PartialFunctionValues, TryValues}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TestRequestTemplates
    extends AnyWordSpec
    with Matchers
    with TryValues
    with PartialFunctionValues {
  private val clientId = "client-id"
  private val clientSecret = SecretString("client-secret")

  private def getTemplates(
      authTemplate: Option[Path] = None,
      tokenTemplate: Option[Path] = None,
      refreshTemplate: Option[Path] = None,
  ): RequestTemplates =
    RequestTemplates(
      clientId = clientId,
      clientSecret = clientSecret,
      authTemplate = authTemplate,
      tokenTemplate = tokenTemplate,
      refreshTemplate = refreshTemplate,
    )

  private def withJsonnetFile(content: String)(testCode: Path => Any): Unit = {
    val file = File.createTempFile("test-request-template", ".jsonnet")
    val writer = new FileWriter(file)
    try {
      writer.write(content)
      writer.close()
      discard(testCode(file.toPath))
    } finally discard(file.delete())
  }

  "the builtin auth template" should {
    "handle empty claims" in {
      val templates = getTemplates()
      val claims = Claims(admin = false, actAs = Nil, readAs = Nil, applicationId = None)
      val requestId = UUID.randomUUID()
      val redirectUri = Uri("https://localhost/cb")
      val params = templates.createAuthRequest(claims, requestId, redirectUri).success.value
      params.keys should contain.only(
        "audience",
        "client_id",
        "redirect_uri",
        "response_type",
        "scope",
        "state",
      )
      params should contain.allOf(
        "audience" -> "https://daml.com/ledger-api",
        "client_id" -> clientId,
        "redirect_uri" -> redirectUri.toString,
        "response_type" -> "code",
        "scope" -> "offline_access",
        "state" -> requestId.toString,
      )
    }
    "handle an admin claim" in {
      val templates = getTemplates()
      val claims = Claims(admin = true, actAs = Nil, readAs = Nil, applicationId = None)
      val requestId = UUID.randomUUID()
      val redirectUri = Uri("https://localhost/cb")
      val params = templates.createAuthRequest(claims, requestId, redirectUri).success.value
      params.keys should contain.only(
        "audience",
        "client_id",
        "redirect_uri",
        "response_type",
        "scope",
        "state",
      )
      params should contain.allOf(
        "audience" -> "https://daml.com/ledger-api",
        "client_id" -> clientId,
        "redirect_uri" -> redirectUri.toString,
        "response_type" -> "code",
        "state" -> requestId.toString,
      )
      val scope = params.valueAt("scope").split(" ")
      scope should contain.allOf("admin", "offline_access")
    }
    "handle actAs claims" in {
      val templates = getTemplates()
      val claims = Claims(
        admin = false,
        actAs = List("Alice", "Bob").map(Ref.Party.assertFromString),
        readAs = Nil,
        applicationId = None,
      )
      val requestId = UUID.randomUUID()
      val redirectUri = Uri("https://localhost/cb")
      val params = templates.createAuthRequest(claims, requestId, redirectUri).success.value
      params.keys should contain.only(
        "audience",
        "client_id",
        "redirect_uri",
        "response_type",
        "scope",
        "state",
      )
      params should contain.allOf(
        "audience" -> "https://daml.com/ledger-api",
        "client_id" -> clientId,
        "redirect_uri" -> redirectUri.toString,
        "response_type" -> "code",
        "state" -> requestId.toString,
      )
      val scope = params.valueAt("scope").split(" ")
      scope should contain.allOf("actAs:Alice", "actAs:Bob", "offline_access")
    }
    "handle readAs claims" in {
      val templates = getTemplates()
      val claims = Claims(
        admin = false,
        actAs = Nil,
        readAs = List("Alice", "Bob").map(Ref.Party.assertFromString),
        applicationId = None,
      )
      val requestId = UUID.randomUUID()
      val redirectUri = Uri("https://localhost/cb")
      val params = templates.createAuthRequest(claims, requestId, redirectUri).success.value
      params.keys should contain.only(
        "audience",
        "client_id",
        "redirect_uri",
        "response_type",
        "scope",
        "state",
      )
      params should contain.allOf(
        "audience" -> "https://daml.com/ledger-api",
        "client_id" -> clientId,
        "redirect_uri" -> redirectUri.toString,
        "response_type" -> "code",
        "state" -> requestId.toString,
      )
      val scope = params.valueAt("scope").split(" ")
      scope should contain.allOf("offline_access", "readAs:Alice", "readAs:Bob")
    }
    "handle an applicationId claim" in {
      val templates = getTemplates()
      val claims = Claims(
        admin = false,
        actAs = Nil,
        readAs = Nil,
        applicationId = Some(Ref.ApplicationId.assertFromString("application-id")),
      )
      val requestId = UUID.randomUUID()
      val redirectUri = Uri("https://localhost/cb")
      val params = templates.createAuthRequest(claims, requestId, redirectUri).success.value
      params.keys should contain.only(
        "audience",
        "client_id",
        "redirect_uri",
        "response_type",
        "scope",
        "state",
      )
      params should contain.allOf(
        "audience" -> "https://daml.com/ledger-api",
        "client_id" -> clientId,
        "redirect_uri" -> redirectUri.toString,
        "response_type" -> "code",
        "state" -> requestId.toString,
      )
      val scope = params.valueAt("scope").split(" ")
      scope should contain.allOf("applicationId:application-id", "offline_access")
    }
  }
  "the builtin token template" should {
    "be complete" in {
      val templates = getTemplates()
      val code = "request-code"
      val redirectUri = Uri("https://localhost/cb")
      val params = templates.createTokenRequest(code, redirectUri).success.value
      params shouldBe Map(
        "client_id" -> clientId,
        "client_secret" -> clientSecret.value,
        "code" -> code,
        "grant_type" -> "authorization_code",
        "redirect_uri" -> redirectUri.toString,
      )
    }
  }
  "the builtin refresh template" should {
    "be complete" in {
      val templates = getTemplates()
      val refreshToken = RefreshToken("refresh-token")
      val params = templates.createRefreshRequest(refreshToken).success.value
      params shouldBe Map(
        "client_id" -> clientId,
        "client_secret" -> clientSecret.value,
        "grant_type" -> "refresh_code",
        "refresh_token" -> refreshToken,
      )
    }
  }
  "user defined templates" should {
    "override the auth template" in withJsonnetFile("""{"key": "value"}""") { templatePath =>
      val templates = getTemplates(authTemplate = Some(templatePath))
      val claims = Claims(admin = false, actAs = Nil, readAs = Nil, applicationId = None)
      val requestId = UUID.randomUUID()
      val redirectUri = Uri("https://localhost/cb")
      val params = templates.createAuthRequest(claims, requestId, redirectUri).success.value
      params shouldBe Map("key" -> "value")
    }
    "override the token template" in withJsonnetFile("""{"key": "value"}""") { templatePath =>
      val templates = getTemplates(tokenTemplate = Some(templatePath))
      val code = "request-code"
      val redirectUri = Uri("https://localhost/cb")
      val params = templates.createTokenRequest(code, redirectUri).success.value
      params shouldBe Map("key" -> "value")
    }
    "override the refresh template" in withJsonnetFile("""{"key": "value"}""") { templatePath =>
      val templates = getTemplates(refreshTemplate = Some(templatePath))
      val refreshToken = RefreshToken("refresh-token")
      val params = templates.createRefreshRequest(refreshToken).success.value
      params shouldBe Map("key" -> "value")
    }
  }
}
