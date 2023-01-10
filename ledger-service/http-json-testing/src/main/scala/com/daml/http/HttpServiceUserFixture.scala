// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.headers.Authorization
import com.daml.http.HttpServiceTestFixture.{authorizationHeader, postRequest}
import com.daml.http.util.ClientUtil.uniqueId
import com.daml.jwt.JwtSigner
import com.daml.jwt.domain.{Jwt, DecodedJwt}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.platform.sandbox.SandboxRequiringAuthorizationFuns
import com.daml.scalautil.ImplicitPreference
import org.scalatest.Suite
import scalaz.syntax.show._
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future}

trait HttpServiceUserFixture extends AkkaBeforeAndAfterAll { this: Suite =>
  protected def testId: String

  // XXX(SC) see #3936 5b52999da2858 and #13113 4af98e1d27efdd
  implicit val `AHS ec`: ExecutionContext with ImplicitPreference[this.type] =
    ImplicitPreference[ExecutionContext, this.type](system.dispatcher)

  def jwtForParties(uri: Uri)(
      actAs: List[domain.Party],
      readAs: List[domain.Party],
      ledgerId: String,
      withoutNamespace: Boolean = false,
      admin: Boolean = false,
  )(implicit ec: ExecutionContext): Future[Jwt]

  protected def jwtAdminNoParty: Jwt

  protected final def headersWithAdminAuth: List[Authorization] =
    authorizationHeader(jwtAdminNoParty)

  protected final def getUniqueParty(name: String) = domain.Party(s"${name}_${uniqueId()}")

}

object HttpServiceUserFixture {
  trait CustomToken extends HttpServiceUserFixture { this: Suite =>
    protected override lazy val jwtAdminNoParty: Jwt = {
      val decodedJwt = DecodedJwt(
        """{"alg": "HS256", "typ": "JWT"}""",
        s"""{"https://daml.com/ledger-api": {"ledgerId": "${testId: String}", "applicationId": "test", "admin": true}}""",
      )
      JwtSigner.HMAC256
        .sign(decodedJwt, "secret")
        .fold(e => fail(s"cannot sign a JWT: ${e.shows}"), identity)
    }

    override final def jwtForParties(uri: Uri)(
        actAs: List[domain.Party],
        readAs: List[domain.Party],
        ledgerId: String,
        withoutNamespace: Boolean = false,
        admin: Boolean = false,
    )(implicit ec: ExecutionContext): Future[Jwt] =
      Future.successful(
        HttpServiceTestFixture.jwtForParties(actAs, readAs, Some(ledgerId), withoutNamespace)
      )
  }

  trait UserToken extends HttpServiceUserFixture with SandboxRequiringAuthorizationFuns {
    this: Suite =>
    override lazy val jwtAdminNoParty: Jwt = Jwt(toHeader(adminTokenStandardJWT))

    override final def jwtForParties(
        uri: Uri
    )(
        actAs: List[domain.Party],
        readAs: List[domain.Party],
        ledgerId: String = "",
        withoutNamespace: Boolean = true,
        admin: Boolean = false,
    )(implicit
        ec: ExecutionContext
    ): Future[Jwt] = {
      val username = getUniqueUserName("test")
      val createUserRequest = domain.CreateUserRequest(
        username,
        None,
        Some(
          Option
            .when(admin)(domain.ParticipantAdmin)
            .toList ++
            actAs.map(domain.CanActAs) ++ readAs.map(domain.CanReadAs)
        ),
      )
      import spray.json._, json.JsonProtocol._
      for {
        _ <- postRequest(
          uri.withPath(Uri.Path("/v1/user/create")),
          createUserRequest.toJson,
          headers = headersWithAdminAuth,
        )
        jwt = jwtForUser(username)
      } yield jwt
    }

    protected final def getUniqueUserName(name: String): String = getUniqueParty(name).unwrap

    protected def jwtForUser(userId: String): Jwt =
      Jwt(toHeader(standardToken(userId)))
  }
}
