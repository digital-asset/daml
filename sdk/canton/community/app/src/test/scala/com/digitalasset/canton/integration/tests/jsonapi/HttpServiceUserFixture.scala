// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.daml.jwt.Jwt
import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.canton.http
import com.digitalasset.canton.http.util.ClientUtil.uniqueId
import com.digitalasset.canton.integration.tests.jsonapi.HttpServiceTestFixture.authorizationHeader
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.CantonFixture
import com.digitalasset.canton.tracing.W3CTraceContext
import org.apache.pekko.http.scaladsl.model.HttpHeader.ParsingResult
import org.apache.pekko.http.scaladsl.model.{HttpHeader, Uri}
import org.scalatest.Suite
import scalaz.syntax.tag.*

import scala.concurrent.{ExecutionContext, Future}

trait HttpServiceUserFixture extends PekkoBeforeAndAfterAll { this: Suite with CantonFixture =>
  protected[this] def jwtUserIdForParties(uri: Uri)(
      actAs: List[http.Party],
      readAs: List[http.Party],
      withoutNamespace: Boolean,
      admin: Boolean,
  )(implicit ec: ExecutionContext): Future[(Jwt, http.UserId)]

  protected def jwtAdminNoParty: Jwt
  // Improves visibility of afterAll which is public in CantonFixture and protected in PekkoBeforeAndAfterAll
  override def afterAll(): Unit = super.afterAll()

  protected[this] def defaultWithoutNamespace = false

  def getTraceContextHeaders(): Seq[HttpHeader] = {

    val nonEmptyTraceContext =
      W3CTraceContext("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")

    extractHeaders(nonEmptyTraceContext)
  }

  protected def extractHeaders(w3cContext: W3CTraceContext): Seq[HttpHeader] =
    w3cContext.asHeaders.toSeq.map(h => HttpHeader.parse(h._1, h._2)).collect {
      case ParsingResult.Ok(header, _) =>
        header
    }

  final def jwtForParties(uri: Uri)(
      actAs: List[http.Party],
      readAs: List[http.Party],
      withoutNamespace: Boolean = defaultWithoutNamespace,
      admin: Boolean = false,
  )(implicit ec: ExecutionContext): Future[Jwt] =
    jwtUserIdForParties(uri)(actAs, readAs, withoutNamespace, admin)(ec) map (_._1)

  protected def headersWithAdminAuth: List[HttpHeader] =
    authorizationHeader(jwtAdminNoParty) ++ getTraceContextHeaders()

  protected final def getUniqueParty(name: String) = http.Party(s"${name}_${uniqueId()}")
}

object HttpServiceUserFixture {

  trait UserToken extends HttpServiceUserFixture {
    this: HttpJsonApiTestBase with HttpTestFuns =>

    protected override lazy val jwtAdminNoParty: Jwt = Jwt(toHeader(adminToken))
    protected[this] override final def defaultWithoutNamespace = true

    protected[this] override final def jwtUserIdForParties(
        uri: Uri
    )(
        actAs: List[http.Party],
        readAs: List[http.Party],
        withoutNamespace: Boolean,
        admin: Boolean,
    )(implicit
        ec: ExecutionContext
    ): Future[(Jwt, http.UserId)] = {
      val username = getUniqueUserName("test")
      val createUserRequest = http.CreateUserRequest(
        username,
        None,
        Some(
          Option
            .when(admin)(http.ParticipantAdmin)
            .toList ++
            actAs.map(http.CanActAs.apply) ++ readAs.map(http.CanReadAs.apply)
        ),
      )
      import spray.json.*, com.digitalasset.canton.http.json.JsonProtocol.*
      postRequest(
        uri.withPath(Uri.Path("/v1/user/create")),
        createUserRequest.toJson,
        headers = headersWithAdminAuth,
      ).map(_ => (jwtForUser(username), http.UserId(username)))
    }

    protected final def getUniqueUserName(name: String): String = getUniqueParty(name).unwrap

    protected def jwtForUser(userId: String): Jwt =
      Jwt(getToken(userId, Some("secret")).value)
  }
}
