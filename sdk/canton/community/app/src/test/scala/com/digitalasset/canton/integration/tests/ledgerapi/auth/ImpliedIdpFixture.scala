// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.user_management_service as ums
import com.digitalasset.canton.integration.TestConsoleEnvironment
import org.scalatest.Assertion

import java.util.UUID
import scala.concurrent.Future

trait ImpliedIdpFixture extends UserManagementAuth with IdentityProviderConfigAuth {
  this: ServiceCallAuthTests =>

  private val idpAdminRight: ums.Right =
    ums.Right(ums.Right.Kind.IdentityProviderAdmin(ums.Right.IdentityProviderAdmin()))
  private val participantAdminRight: ums.Right =
    ums.Right(ums.Right.Kind.ParticipantAdmin(ums.Right.ParticipantAdmin()))

  /** Prior to running a test case this fixture creates a user with IdpAdmin right in a non-default
    * idp and passes this user's token to the test case
    */
  def impliedIdpWithIdpAdminFixture(nonDefaultIdpId: String)(
      testCase: ServiceCallContext => Future[(List[String], List[String])]
  )(implicit env: TestConsoleEnvironment): Assertion = {
    require(nonDefaultIdpId.trim.nonEmpty)
    import env.*
    expectSuccess(for {
      idpConfig <- createConfig(canBeAnAdmin, idpId = Some(nonDefaultIdpId))
      tokenIssuer1 = idpConfig.issuer
      userId = "user-idp-admin-" + UUID.randomUUID().toString
      (_, context) <- createUserByAdminRSA(
        privateKey = key1.privateKey,
        keyId = key1.id,
        userId = userId,
        identityProviderId = nonDefaultIdpId,
        tokenIssuer = Some(tokenIssuer1),
        rights = Vector(idpAdminRight),
      )
      _ = context.identityProviderId shouldBe nonDefaultIdpId withClue "idp id of an idp admin"
      (parties, users) <- testCase(context)
      // cleanup the idp configuration we created in order to prevent exceeding the max number of possible idp configs
      _ <- parkParties(canBeAnAdmin, parties, nonDefaultIdpId)
      _ <- parkUsers(canBeAnAdmin, users :+ userId, nonDefaultIdpId)
      _ <- deleteConfig(canBeAnAdmin, identityProviderId = nonDefaultIdpId)
    } yield ())
  }

  /** Prior to running a test case this fixture creates a regular user (no IdpAdmin or
    * ParticipantAdmin rights) in a non-default idp and passes this user's token to the test case
    */
  def impliedIdpWithRegularUserFixture(nonDefaultIdpId: String, userId: String)(
      testCase: ServiceCallContext => Future[Assertion]
  )(implicit env: TestConsoleEnvironment): Assertion = {
    require(nonDefaultIdpId.trim.nonEmpty)
    import env.*
    expectSuccess(for {
      idpConfig <- createConfig(canBeAnAdmin, idpId = Some(nonDefaultIdpId))
      tokenIssuer1 = idpConfig.issuer
      (_, context) <- createUserByAdminRSA(
        privateKey = key1.privateKey,
        keyId = key1.id,
        userId = userId,
        identityProviderId = nonDefaultIdpId,
        tokenIssuer = Some(tokenIssuer1),
        rights = Vector.empty,
      )
      _ = context.identityProviderId shouldBe nonDefaultIdpId withClue "idp id of a regular user"
      _ <- testCase(context)
      // cleanup the idp configuration we created in order to prevent exceeding the max number of possible idp configs
      _ <- parkUsers(canBeAnAdmin, List(userId), nonDefaultIdpId)
      _ <- deleteConfig(canBeAnAdmin, identityProviderId = nonDefaultIdpId)
    } yield ())
  }

  /** Prior to running a test cases this fixture creates a user with ParticipantAdmin right in the
    * default idp and passes this user's token to the test case
    */
  def impliedIdpWithParticipantAdminFixture(nonDefaultIdpId: String)(
      testCase: ServiceCallContext => Future[Assertion]
  )(implicit env: TestConsoleEnvironment): Assertion = {
    import env.*
    expectSuccess(for {
      response1 <- createConfig(canBeAnAdmin, idpId = Some(nonDefaultIdpId))
      (_, context) <- createUserByAdmin(
        userId = "user-admin-" + UUID.randomUUID().toString,
        identityProviderId = "",
        tokenIssuer = None,
        rights = Vector(participantAdminRight),
      )
      _ = context.identityProviderId shouldBe "" withClue "idp id of a participant admin"
      _ <- testCase(context)
      // cleanup the idp configuration we created in order to prevent exceeding the max number of possible idp configs
      _ <- deleteConfig(canBeAnAdmin, identityProviderId = nonDefaultIdpId)
    } yield ())
  }

}
