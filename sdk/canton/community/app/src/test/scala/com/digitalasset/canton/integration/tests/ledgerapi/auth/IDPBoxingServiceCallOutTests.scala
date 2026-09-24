// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.identity_provider_config_service.IdentityProviderConfig
import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v2.admin.user_management_service.User
import com.daml.ledger.api.v2.admin.{
  party_management_service as pproto,
  user_management_service as uproto,
}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.AuthServiceJWTSuppressionRule

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

trait IDPBoxingServiceCallOutTests
    extends ServiceCallAuthTests
    with IdentityProviderConfigAuth
    with ErrorsAssertions {

  override protected def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] = ???

  protected def boxedCall(
      userId: String,
      serviceCallContext: ServiceCallContext,
      rights: Vector[uproto.Right],
  )(implicit ec: ExecutionContext): Future[Any]

  protected def createUser(
      userId: String,
      serviceCallContext: ServiceCallContext,
      rights: Vector[uproto.Right],
  ): Future[uproto.CreateUserResponse] = {
    val user = uproto.User(
      id = userId,
      primaryParty = "",
      isDeactivated = false,
      metadata = Some(ObjectMeta.defaultInstance),
      identityProviderId = serviceCallContext.identityProviderId,
    )
    val req = uproto.CreateUserRequest(Some(user), rights)
    stub(uproto.UserManagementServiceGrpc.stub(channel), serviceCallContext.token)
      .createUser(req)
  }

  private def allocateParty(party: String, identityProviderId: String = "")(implicit
      ec: ExecutionContext
  ): Future[String] =
    stub(pproto.PartyManagementServiceGrpc.stub(channel), canBeAnAdmin.token)
      .allocateParty(
        pproto.AllocatePartyRequest(
          partyIdHint = party,
          localMetadata = None,
          identityProviderId = identityProviderId,
          synchronizerId = "",
          userId = "",
        )
      )
      .map(_.partyDetails.value.party)

  private def createIDPBundle(context: ServiceCallContext, suffix: String)(implicit
      ec: ExecutionContext
  ): Future[(User, ServiceCallContext, IdentityProviderConfig)] =
    for {
      idpConfig <- createConfig(context)
      (user, idpAdminContext) <- createUserByAdminRSA(
        userId = "idp-admin-" + suffix,
        identityProviderId = idpConfig.identityProviderId,
        tokenIssuer = Some(idpConfig.issuer),
        rights = idpAdminRights,
        privateKey = key1.privateKey,
        keyId = key1.id,
      )
    } yield (user, idpAdminContext, idpConfig)

  serviceCallName should {
    "deny IDP Admin granting permissions to parties which do not exist" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "Grant rights to non existing parties")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnknownResource {
          val suffix = UUID.randomUUID().toString
          for {
            (_, idpAdminContext, _) <- createIDPBundle(canBeAnAdmin, suffix)
            readAsParty = s"read-$suffix"
            actAsParty = s"act-$suffix"

            _ <- boxedCall(
              "user-" + suffix,
              idpAdminContext,
              readAndActRights(readAsParty, actAsParty),
            )
          } yield ()
        }
      }
    }

    "allow IDP Admin granting permissions to parties which are allocated in the same IDP" taggedAs adminSecurityAsset
      .setHappyCase(
        "IDP admin can grant user rights to parties in the same IDP"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectSuccess {
          val suffix = UUID.randomUUID().toString
          for {
            (_, idpAdminContext, idpConfig) <- createIDPBundle(canBeAnAdmin, suffix)
            readAsParty <- allocateParty(s"read-$suffix", idpConfig.identityProviderId)
            actAsParty <- allocateParty(s"act-$suffix", idpConfig.identityProviderId)

            _ <- boxedCall(
              "user-" + suffix,
              idpAdminContext,
              readAndActRights(readAsParty, actAsParty),
            )
          } yield ()
        }
      }
    }

    "deny IDP Admin granting permissions to parties which are outside of any IDP" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "Grant user rights to parties outside of any IDP")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnknownResource {
          val suffix = UUID.randomUUID().toString
          for {
            (_, idpAdminContext, _) <- createIDPBundle(canBeAnAdmin, suffix)
            readAsParty <- allocateParty(s"read-$suffix")
            actAsParty <- allocateParty(s"act-$suffix")

            _ <- boxedCall(
              "user-" + suffix,
              idpAdminContext,
              readAndActRights(readAsParty, actAsParty),
            )
          } yield ()
        }
      }
    }

    "deny IDP Admin granting permissions in other IDPs" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "Grant rights to parties in other IDPs")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnknownResource {
          val suffix = UUID.randomUUID().toString
          for {
            (_, idpAdminContext, _) <- createIDPBundle(canBeAnAdmin, suffix)
            otherIdpConfig <- createConfig(canBeAnAdmin)
            readAsParty <- allocateParty(s"read-$suffix", otherIdpConfig.identityProviderId)
            actAsParty <- allocateParty(s"act-$suffix", otherIdpConfig.identityProviderId)

            _ <- boxedCall(
              "user-" + suffix,
              idpAdminContext,
              readAndActRights(readAsParty, actAsParty),
            )
          } yield ()
        }
      }
    }

    "deny Admin granting permissions to parties which do not exist" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "Grant rights to non existing parties")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnknownResource {
          val suffix = UUID.randomUUID().toString
          for {
            (_, idpAdminContext, _) <- createIDPBundle(canBeAnAdmin, suffix)
            readAsParty = s"read-$suffix"
            actAsParty = s"act-$suffix"

            _ <- boxedCall(
              "user-" + suffix,
              canBeAnAdmin,
              readAndActRights(readAsParty, actAsParty),
            )
          } yield ()
        }
      }
    }

    "allow Admin granting permissions to parties which are allocated in the same IDP" taggedAs adminSecurityAsset
      .setHappyCase(
        "Grant user rights to parties in the same IDP"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectSuccess {
          val suffix = UUID.randomUUID().toString
          for {
            (_, idpAdminContext, idpConfig) <- createIDPBundle(canBeAnAdmin, suffix)
            readAsParty <- allocateParty(s"read-$suffix", idpConfig.identityProviderId)
            actAsParty <- allocateParty(s"act-$suffix", idpConfig.identityProviderId)

            _ <- boxedCall(
              "user-" + suffix,
              canBeAnAdmin,
              readAndActRights(readAsParty, actAsParty),
            )
          } yield ()
        }
      }
    }

    "allow Admin granting permissions to parties which are outside of any IDP" taggedAs adminSecurityAsset
      .setHappyCase(
        "Grant user rights to parties outside of any IDP"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectSuccess {
          val suffix = UUID.randomUUID().toString
          for {
            (_, idpAdminContext, _) <- createIDPBundle(canBeAnAdmin, suffix)
            readAsParty <- allocateParty(s"read-$suffix")
            actAsParty <- allocateParty(s"act-$suffix")

            _ <- boxedCall(
              "user-" + suffix,
              canBeAnAdmin,
              readAndActRights(readAsParty, actAsParty),
            )
          } yield ()
        }
      }
    }

    "allow Admin granting permissions in other IDPs" taggedAs adminSecurityAsset
      .setHappyCase(
        "Grant rights to parties in other IDPs"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectSuccess {
          val suffix = UUID.randomUUID().toString
          for {
            (_, idpAdminContext, _) <- createIDPBundle(canBeAnAdmin, suffix)
            otherIdpConfig <- createConfig(canBeAnAdmin)
            readAsParty <- allocateParty(s"read-$suffix", otherIdpConfig.identityProviderId)
            actAsParty <- allocateParty(s"act-$suffix", otherIdpConfig.identityProviderId)

            _ <- boxedCall(
              "user-" + suffix,
              canBeAnAdmin,
              readAndActRights(readAsParty, actAsParty),
            )
          } yield ()
        }
      }
    }
  }

  protected def idpAdminRights: Vector[uproto.Right] = Vector(
    uproto.Right(
      uproto.Right.Kind.IdentityProviderAdmin(uproto.Right.IdentityProviderAdmin())
    )
  )
  protected def readAndActRights(readAsParty: String, actAsParty: String): Vector[uproto.Right] =
    Vector(
      uproto.Right(
        uproto.Right.Kind.CanReadAs(uproto.Right.CanReadAs(readAsParty))
      ),
      uproto.Right(uproto.Right.Kind.CanActAs(uproto.Right.CanActAs(actAsParty))),
    )
}
