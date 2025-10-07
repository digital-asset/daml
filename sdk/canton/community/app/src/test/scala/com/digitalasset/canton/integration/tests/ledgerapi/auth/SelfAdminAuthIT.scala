// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v2.admin.party_management_service.GetPartiesRequest
import com.daml.ledger.api.v2.admin.user_management_service.{GetUserRequest, ListUserRightsRequest}
import com.daml.ledger.api.v2.admin.{
  party_management_service as pproto,
  user_management_service as uproto,
}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.{
  ApiPartyManagementServiceSuppressionRule,
  AuthServiceJWTSuppressionRule,
}
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentDefinition,
  TestConsoleEnvironment,
}
import monocle.Monocle.toAppliedFocusOps

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class SelfAdminAuthIT
    extends ServiceCallAuthTests
    with IdentityProviderConfigAuth
    with ErrorsAssertions {

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.addConfigTransform(
      ConfigTransforms.updateAllParticipantConfigs_(
        _.focus(_.ledgerApi.partyManagementService.maxSelfAllocatedParties)
          .replace(NonNegativeInt.tryCreate(2))
      )
    )

  override protected def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] = ???

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

  private def allocateParty(
      party: String,
      serviceCallContext: ServiceCallContext,
      userId: String = "",
      identityProviderIdOverride: Option[String] = None,
  )(implicit
      ec: ExecutionContext
  ): Future[String] =
    stub(pproto.PartyManagementServiceGrpc.stub(channel), serviceCallContext.token)
      .allocateParty(
        pproto.AllocatePartyRequest(
          partyIdHint = party,
          localMetadata = None,
          identityProviderId =
            identityProviderIdOverride.getOrElse(serviceCallContext.identityProviderId),
          synchronizerId = "",
          userId = userId,
        )
      )
      .map(_.partyDetails.value.party)

  serviceCallName should {

    "allow user querying about own parties" taggedAs adminSecurityAsset
      .setHappyCase(
        "User can query the details of the own parties"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectSuccess {
          val suffix = UUID.randomUUID().toString
          for {
            read <- allocateParty(s"read-$suffix", asAdmin)
            act <- allocateParty(s"act-$suffix", asAdmin)
            execute <- allocateParty(s"execute-$suffix", asAdmin)

            inputParties = Seq(read, act, execute)

            (_, user1Context) <- createUserByAdmin(
              "user-1-" + suffix,
              rights = Vector(
                uproto.Right(uproto.Right.Kind.CanReadAs(uproto.Right.CanReadAs(read))),
                uproto.Right(uproto.Right.Kind.CanActAs(uproto.Right.CanActAs(act))),
                uproto.Right(uproto.Right.Kind.CanExecuteAs(uproto.Right.CanExecuteAs(execute))),
              ),
            )
            (_, user2Context) <- createUserByAdmin(
              "user-2-" + suffix,
              rights = Vector(
                uproto.Right(uproto.Right.Kind.CanReadAsAnyParty(uproto.Right.CanReadAsAnyParty())),
                uproto.Right(uproto.Right.Kind.CanActAs(uproto.Right.CanActAs(act))),
                uproto.Right(uproto.Right.Kind.CanExecuteAs(uproto.Right.CanExecuteAs(execute))),
              ),
            )
            (_, user3Context) <- createUserByAdmin(
              "user-3-" + suffix,
              rights = Vector(
                uproto.Right(uproto.Right.Kind.CanReadAs(uproto.Right.CanReadAs(read))),
                uproto.Right(uproto.Right.Kind.CanActAs(uproto.Right.CanActAs(act))),
                uproto.Right(
                  uproto.Right.Kind.CanExecuteAsAnyParty(uproto.Right.CanExecuteAsAnyParty())
                ),
              ),
            )

            parties1 <- stub(pproto.PartyManagementServiceGrpc.stub(channel), user1Context.token)
              .getParties(
                GetPartiesRequest(parties = inputParties, identityProviderId = "")
              )
            parties2 <- stub(pproto.PartyManagementServiceGrpc.stub(channel), user2Context.token)
              .getParties(
                GetPartiesRequest(parties = inputParties, identityProviderId = "")
              )
            parties3 <- stub(pproto.PartyManagementServiceGrpc.stub(channel), user3Context.token)
              .getParties(
                GetPartiesRequest(parties = inputParties, identityProviderId = "")
              )

          } yield (
            parties1.partyDetails.map(_.party) should contain theSameElementsAs inputParties,
            parties2.partyDetails.map(_.party) should contain theSameElementsAs inputParties,
            parties3.partyDetails.map(_.party) should contain theSameElementsAs inputParties,
          )
        }
      }
    }

    "deny user querying about parties it doesn't own" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "User querying status of parties it doesn't own")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectPermissionDenied {
          val suffix = UUID.randomUUID().toString
          for {
            read <- allocateParty(s"read-$suffix", asAdmin)
            act <- allocateParty(s"act-$suffix", asAdmin)
            execute <- allocateParty(s"execute-$suffix", asAdmin)

            inputParties = Seq(read, act, execute)

            (_, userContext) <- createUserByAdmin(
              "user-" + suffix,
              rights = Vector(
                uproto.Right(uproto.Right.Kind.CanReadAs(uproto.Right.CanReadAs(read))),
                uproto.Right(uproto.Right.Kind.CanActAs(uproto.Right.CanActAs(act))),
              ),
            )

            _ <- stub(pproto.PartyManagementServiceGrpc.stub(channel), userContext.token)
              .getParties(
                GetPartiesRequest(parties = inputParties, identityProviderId = "")
              )

          } yield ()
        }
      }
    }

    "allow user querying own records" taggedAs adminSecurityAsset
      .setHappyCase(
        "User can query own records"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectSuccess {
          val suffix = UUID.randomUUID().toString
          for {
            (user, userContext) <- createUserByAdmin("user-" + suffix)
            _ <- stub(uproto.UserManagementServiceGrpc.stub(channel), userContext.token).getUser(
              GetUserRequest(user.id, "")
            )
            _ <- stub(uproto.UserManagementServiceGrpc.stub(channel), userContext.token)
              .listUserRights(
                ListUserRightsRequest(user.id, "")
              )
              .map(_.rights)

          } yield ()
        }
      }
    }

    "deny user querying someone else's records" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "User querying records of other users")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectPermissionDenied {
          val suffix = UUID.randomUUID().toString
          for {
            (_, userContext) <- createUserByAdmin("user-1-" + suffix)
            (anotherUser, _) <- createUserByAdmin("user-2-" + suffix)
            _ <- stub(uproto.UserManagementServiceGrpc.stub(channel), userContext.token).getUser(
              GetUserRequest(anotherUser.id, "")
            )
            _ <- stub(uproto.UserManagementServiceGrpc.stub(channel), userContext.token)
              .listUserRights(
                ListUserRightsRequest(anotherUser.id, "")
              )
              .map(_.rights)
          } yield ()
        }
      }
    }

    "allow user allocating own party" taggedAs adminSecurityAsset
      .setHappyCase(
        "User can allocate own party"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectSuccess {
          val suffix = UUID.randomUUID().toString
          for {
            (user, userContext) <- createUserByAdmin("user-" + suffix)
            party <- allocateParty(s"party-$suffix", userContext, userId = user.id)
            parties <- stub(pproto.PartyManagementServiceGrpc.stub(channel), userContext.token)
              .getParties(
                GetPartiesRequest(parties = Seq(party), identityProviderId = "")
              )
            rights <- stub(uproto.UserManagementServiceGrpc.stub(channel), userContext.token)
              .listUserRights(
                ListUserRightsRequest(user.id, "")
              )
              .map(_.rights)

          } yield (
            parties.partyDetails.map(_.party) should contain theSameElementsAs Seq(party),
            rights should contain theSameElementsAs Vector(
              uproto.Right(uproto.Right.Kind.CanActAs(uproto.Right.CanActAs(party)))
            ),
          )
        }
      }
    }

    "deny user allocating party for another user" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "User allocating party for another user")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectPermissionDenied {
          val suffix = UUID.randomUUID().toString
          for {
            (_, userContext) <- createUserByAdmin("user-1-" + suffix)
            (anotherUser, _) <- createUserByAdmin("user-2-" + suffix)
            _ <- allocateParty(s"party-$suffix", userContext, userId = anotherUser.id)
          } yield ()
        }
      }
    }

    "deny user allocating party without specifying who will own it" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "User allocating party without specifying who will own it")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectPermissionDenied {
          val suffix = UUID.randomUUID().toString
          for {
            (_, userContext) <- createUserByAdmin("user-" + suffix)
            _ <- allocateParty(s"party-$suffix", userContext)
          } yield ()
        }
      }
    }

    "deny user allocating party above the quota" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "User allocating party above the quota")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(
        AuthServiceJWTSuppressionRule || ApiPartyManagementServiceSuppressionRule
      ) {
        expectPermissionDenied {
          val suffix = UUID.randomUUID().toString
          for {
            (user, userContext) <- createUserByAdmin("user-1-" + suffix)
            _ <- allocateParty(s"party-1-$suffix", userContext, userId = user.id)
            _ <- allocateParty(s"party-2-$suffix", userContext, userId = user.id)
            _ <- allocateParty(s"party-3-$suffix", userContext, userId = user.id)
          } yield ()
        }
      }
    }

    "deny user allocating party above the quota, when other parties added by admin" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "User allocating party above the quota")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(
        AuthServiceJWTSuppressionRule || ApiPartyManagementServiceSuppressionRule
      ) {
        expectPermissionDenied {
          val suffix = UUID.randomUUID().toString
          for {
            party1 <- allocateParty(s"party-1-$suffix", asAdmin)
            party2 <- allocateParty(s"party-2-$suffix", asAdmin)

            (user, userContext) <- createUserByAdmin(
              "user-1-" + suffix,
              rights = Vector(
                uproto.Right(uproto.Right.Kind.CanReadAs(uproto.Right.CanReadAs(party1))),
                uproto.Right(uproto.Right.Kind.CanActAs(uproto.Right.CanActAs(party2))),
              ),
            )
            _ <- allocateParty(s"party-3-$suffix", userContext, userId = user.id)
          } yield ()
        }
      }
    }

    "allow user allocating party below the quota, when rights contain few distinctive parties" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "User allocating party above the quota")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(
        AuthServiceJWTSuppressionRule || ApiPartyManagementServiceSuppressionRule
      ) {
        expectSuccess {
          val suffix = UUID.randomUUID().toString
          for {
            party1 <- allocateParty(s"party-1-$suffix", asAdmin)

            (user, userContext) <- createUserByAdmin(
              "user-1-" + suffix,
              rights = Vector(
                uproto.Right(uproto.Right.Kind.CanReadAs(uproto.Right.CanReadAs(party1))),
                uproto.Right(uproto.Right.Kind.CanActAs(uproto.Right.CanActAs(party1))),
              ),
            )
            _ <- allocateParty(s"party-3-$suffix", userContext, userId = user.id)
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

  override def serviceCallName: String = "GetParties"
}
