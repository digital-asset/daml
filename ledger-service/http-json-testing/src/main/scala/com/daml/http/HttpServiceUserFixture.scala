// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.http.scaladsl.model.headers.Authorization
import com.daml.http.HttpServiceTestFixture.{authorizationHeader, postRequest}
import com.daml.http.util.ClientUtil.uniqueId
import com.daml.integrationtest.CantonRunner
import com.daml.jwt.JwtSigner
import com.daml.jwt.domain.{DecodedJwt, Jwt}
import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.scalautil.ImplicitPreference
import org.scalatest.OptionValues._
import org.scalatest.Suite
import scalaz.syntax.show._
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future}

trait HttpServiceUserFixture extends PekkoBeforeAndAfterAll { this: Suite =>
  protected def testId: String

  // XXX(SC) see #3936 5b52999da2858 and #13113 4af98e1d27efdd
  implicit val `AHS ec`: ExecutionContext with ImplicitPreference[this.type] =
    ImplicitPreference[ExecutionContext, this.type](system.dispatcher)

  protected[this] def jwtAppIdForParties(uri: Uri)(
      actAs: List[domain.Party],
      readAs: List[domain.Party],
      ledgerId: String,
      withoutNamespace: Boolean,
      admin: Boolean,
  )(implicit ec: ExecutionContext): Future[(Jwt, domain.ApplicationId)]

  protected[this] def defaultWithoutNamespace = false

  final def jwtForParties(uri: Uri)(
      actAs: List[domain.Party],
      readAs: List[domain.Party],
      ledgerId: String,
      withoutNamespace: Boolean = defaultWithoutNamespace,
      admin: Boolean = false,
  )(implicit ec: ExecutionContext): Future[Jwt] =
    jwtAppIdForParties(uri)(actAs, readAs, ledgerId, withoutNamespace, admin)(ec) map (_._1)

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
        s"""{"https://daml.com/ledger-api": {"ledgerId": "participant0", "applicationId": "test", "admin": true}}""",
      )
      JwtSigner.HMAC256
        .sign(decodedJwt, "secret")
        .fold(e => fail(s"cannot sign a JWT: ${e.shows}"), identity)
    }

    protected[this] override final def jwtAppIdForParties(uri: Uri)(
        actAs: List[domain.Party],
        readAs: List[domain.Party],
        ledgerId: String,
        withoutNamespace: Boolean,
        admin: Boolean,
    )(implicit ec: ExecutionContext): Future[(Jwt, domain.ApplicationId)] =
      Future.successful(
        (
          HttpServiceTestFixture.jwtForParties(actAs, readAs, Some(ledgerId), withoutNamespace),
          HttpServiceTestFixture.applicationId,
        )
      )
  }

  trait UserToken extends HttpServiceUserFixture {
    this: Suite =>
    override lazy val jwtAdminNoParty: Jwt = Jwt(
      CantonRunner.getToken("participant_admin", Some("secret")).value
    )

    protected[this] override final def defaultWithoutNamespace = true

    protected[this] override final def jwtAppIdForParties(
        uri: Uri
    )(
        actAs: List[domain.Party],
        readAs: List[domain.Party],
        ledgerId: String,
        withoutNamespace: Boolean,
        admin: Boolean,
    )(implicit
        ec: ExecutionContext
    ): Future[(Jwt, domain.ApplicationId)] = {
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
      postRequest(
        uri.withPath(Uri.Path("/v1/user/create")),
        createUserRequest.toJson,
        headers = headersWithAdminAuth,
      ).map(_ => (jwtForUser(username), domain.ApplicationId(username)))
    }

    protected final def getUniqueUserName(name: String): String = getUniqueParty(name).unwrap

    protected def jwtForUser(userId: String): Jwt =
      Jwt(CantonRunner.getToken(userId, Some("secret")).value)
  }
}
