// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.http.scaladsl.model.{StatusCodes, Uri}
import com.daml.fetchcontracts.domain.TemplateId.OptionalPkg
import com.daml.http.HttpServiceTestFixture.{UseTls, authorizationHeader, getResult}
import com.daml.ledger.client.withoutledgerid.{LedgerClient => DamlLedgerClient}
import com.daml.http.dbbackend.JdbcConfig
import com.daml.http.json.JsonProtocol._
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.auth.StandardJWTPayload
import com.daml.ledger.api.domain.{User, UserRight}
import com.daml.ledger.api.domain.UserRight.CanActAs
import com.daml.ledger.api.v1.{value => v}
import com.daml.lf.data.Ref
import com.daml.platform.sandbox.SandboxRequiringAuthorization
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.Inside
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import scalaz.syntax.show._
import spray.json.JsValue

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class HttpServiceIntegrationTestUserManagement
    extends AsyncFreeSpec
    with Matchers
    with Inside
    with StrictLogging
    with AbstractHttpServiceIntegrationTestFuns
    with SandboxRequiringAuthorization {

  override def jdbcConfig: Option[JdbcConfig] = None

  override def staticContentConfig: Option[StaticContentConfig] = None

  override def useTls: UseTls = UseTls.Tls

  override def wsConfig: Option[WebsocketConfig] = None

  import scalaz.syntax.tag._

  def jwtForUser(userId: String, admin: Boolean = false) =
    Jwt(toHeader(StandardJWTPayload(standardToken(userId).payload.copy(admin = admin))))

  val participantAdminJwt: Jwt = Jwt(toHeader(adminTokenStandardJWT))

  def createUser(ledgerClient: DamlLedgerClient)(
      userId: Ref.UserId,
      primaryParty: Option[Ref.Party] = None,
      initialRights: List[UserRight] = List.empty,
  ): Future[User] =
    ledgerClient.userManagementClient.createUser(
      User(userId, primaryParty),
      initialRights,
      Some(participantAdminJwt.value),
    )

  def headersWithUserAuth(userId: String) =
    authorizationHeader(jwtForUser(userId))

  def getUniqueUserName(name: String) = getUniqueParty(name).toString

  "create IOU should work with correct user rights" in withHttpServiceAndClient(
    participantAdminJwt
  ) { (uri, encoder, _, ledgerClient, _) =>
    logger.info("allocating party")
    val alice = getUniqueParty("Alice")
    val command: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand(alice.unwrap)
    val input: JsValue = encoder.encodeCreateCommand(command).valueOr(e => fail(e.shows))
    logger.info("Trying to create user")
    for {
      user <- createUser(ledgerClient)(
        Ref.UserId.assertFromString(getUniqueUserName("nice.user")),
        initialRights = List(
          CanActAs(Ref.Party.assertFromString(alice.toString))
        ),
      )
      _ = logger.info("successfully created user")
      (status, output) <- postJsonRequest(
        uri.withPath(Uri.Path("/v1/create")),
        input,
        headers = headersWithUserAuth(user.id),
      )
      assertion <- {
        status shouldBe StatusCodes.OK
        assertStatus(output, StatusCodes.OK)
        val activeContract = getResult(output)
        assertActiveContract(activeContract)(command, encoder)
      }
    } yield assertion
  }

  "create IOU should fail if user has no permission" in withHttpServiceAndClient(
    participantAdminJwt
  ) { (uri, encoder, _, ledgerClient, _) =>
    logger.info("allocating party")
    val alice = getUniqueParty("Alice")
    val bob = getUniqueParty("Bob")
    val command: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand(alice.unwrap)
    val input: JsValue = encoder.encodeCreateCommand(command).valueOr(e => fail(e.shows))
    logger.info("Trying to create user")
    for {
      user <- createUser(ledgerClient)(
        Ref.UserId.assertFromString(getUniqueUserName("nice.user")),
        initialRights = List(
          CanActAs(Ref.Party.assertFromString(bob.toString))
        ),
      )
      _ = logger.info("successfully created user")
      (status, output) <- postJsonRequest(
        uri.withPath(Uri.Path("/v1/create")),
        input,
        headers = headersWithUserAuth(user.id),
      )
      assertion <- {
        status shouldBe StatusCodes.BadRequest
        assertStatus(output, StatusCodes.BadRequest)
      }
    } yield assertion
  }
}
