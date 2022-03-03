// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.headers.Authorization
import akka.stream.Materializer
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.http.HttpServiceTestFixture.{authorizationHeader, postRequest}
import com.daml.http.util.ClientUtil.uniqueId
import com.daml.jwt.JwtSigner
import com.daml.jwt.domain.{Jwt, DecodedJwt}
import com.daml.platform.sandbox.SandboxRequiringAuthorizationFuns
import scalaz.syntax.show._
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future}

trait HttpServiceUserFixture {
  protected def testId: String

  // TODO(SC) we make asys, mat, and aesf "win" to match the prior behavior when
  // these defns were in AbstractHttpServiceIntegrationTestFuns.  But it
  // might be possible to phase them out entirely in favor of AkkaBeforeAndAfterAll
  import shapeless.tag, tag.@@ // used for subtyping to make `AHS ec` beat executionContext
  implicit val `AHS asys`: ActorSystem @@ this.type = tag[this.type] {
    import com.typesafe.config.ConfigFactory
    val customConf = ConfigFactory.parseString("""
      akka.http.server.request-timeout = 60s
    """)
    ActorSystem(testId, ConfigFactory.load(customConf))
  }
  implicit val `AHS mat`: Materializer @@ this.type = tag[this.type](Materializer(`AHS asys`))
  implicit val `AHS aesf`: ExecutionSequencerFactory @@ this.type =
    tag[this.type](new AkkaExecutionSequencerPool(testId)(`AHS asys`))
  // XXX(SC) `AHS ec` has beat executionContext for much longer
  implicit val `AHS ec`: ExecutionContext @@ this.type = tag[this.type](`AHS asys`.dispatcher)

  def jwtForParties(uri: Uri)(
      actAs: List[String],
      readAs: List[String],
      ledgerId: String,
      withoutNamespace: Boolean = false,
      admin: Boolean = false,
  )(implicit ec: ExecutionContext): Future[Jwt]

  protected val jwtAdminNoParty: Jwt

  protected final def headersWithAdminAuth: List[Authorization] =
    authorizationHeader(jwtAdminNoParty)

  protected final def getUniqueParty(name: String) = domain.Party(s"${name}_${uniqueId()}")

}

object HttpServiceUserFixture {
  trait CustomToken extends HttpServiceUserFixture { this: org.scalatest.Assertions =>
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
        actAs: List[String],
        readAs: List[String],
        ledgerId: String,
        withoutNamespace: Boolean = false,
        admin: Boolean = false,
    )(implicit ec: ExecutionContext): Future[Jwt] =
      Future.successful(
        HttpServiceTestFixture.jwtForParties(actAs, readAs, ledgerId, withoutNamespace)
      )
  }

  trait UserToken extends HttpServiceUserFixture with SandboxRequiringAuthorizationFuns {
    override lazy val jwtAdminNoParty: Jwt = Jwt(toHeader(adminTokenStandardJWT))

    override final def jwtForParties(
        uri: Uri
    )(
        actAs: List[String],
        readAs: List[String],
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
            actAs.map(it => domain.CanActAs(domain.Party(it))) ++
            readAs.map(it => domain.CanReadAs(domain.Party(it)))
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
